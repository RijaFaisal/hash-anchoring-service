package com.hashanchor.blockchain;

import com.hashanchor.blockchain.generated.HashAnchor;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.web3j.abi.EventEncoder;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.tuples.generated.Tuple2;
import org.web3j.tx.ChainIdLong;
import org.web3j.tx.RawTransactionManager;
import org.web3j.tx.gas.DefaultGasProvider;
import org.web3j.tx.response.PollingTransactionReceiptProcessor;
import org.web3j.utils.Numeric;

/**
 * Talks to the deployed {@code HashAnchor} contract through the generated
 * {@link HashAnchor} wrapper (see {@code build.gradle.kts}'s
 * {@code generateContractWrappers} task — that class isn't hand-written,
 * it's regenerated from the contract's ABI every build).
 *
 * <p>Every wrapper method returns a {@code RemoteFunctionCall<T>}, not a
 * plain value. It's lazy — building it does nothing on its own — and
 * calling {@code .send()} is what actually talks to the chain and blocks
 * the calling thread until it's done:
 *
 * <ul>
 *   <li>For {@link #anchor}, a state-changing call, {@code .send()} signs a
 *       transaction with our {@link Credentials}, submits it, then polls
 *       the node until it lands in a block and returns the
 *       {@link TransactionReceipt} — this is the "wait for confirmation"
 *       step.
 *   <li>For {@link #verify}, a {@code view} function, {@code .send()} just
 *       does a read-only {@code eth_call} and returns immediately with the
 *       decoded result — no transaction, no waiting for a block.
 * </ul>
 *
 * <p>If no receipt shows up within {@code receipt-timeout}, {@code .send()}
 * throws a {@code TransactionException}. That doesn't mean the transaction
 * failed — it may still be mined later — which is why the caller checks
 * {@link #verify} before treating it as a failure.
 *
 * <p>No retry or "already anchored" handling lives here — that's
 * {@link com.hashanchor.domain.AnchoringService}'s job. This class only
 * wraps the two raw contract calls.
 */
@Component
public class AnchorClient {

    private final Web3j web3j;
    private final String contractAddress;
    private final HashAnchor contract;

    public AnchorClient(
            Web3j web3j,
            Credentials credentials,
            @Value("${hashanchor.blockchain.contract-address}") String contractAddress,
            @Value("${hashanchor.blockchain.receipt-poll-interval}") Duration receiptPollInterval,
            @Value("${hashanchor.blockchain.receipt-timeout}") Duration receiptTimeout) {
        this.web3j = web3j;
        this.contractAddress = contractAddress;
        // web3j's default receipt wait is 40 polls x 15s = 10 minutes, which
        // is longer than Kafka's max.poll.interval.ms (5 minutes): the
        // broker would decide the consumer had died mid-wait and hand its
        // partition to someone else. Bounding it here keeps a single anchor
        // attempt well inside that limit. ChainIdLong.NONE matches what
        // the plain (web3j, credentials) constructor used before.
        int attempts = (int) Math.max(1, receiptTimeout.toMillis() / receiptPollInterval.toMillis());
        var transactionManager = new RawTransactionManager(
                web3j,
                credentials,
                ChainIdLong.NONE,
                new PollingTransactionReceiptProcessor(web3j, receiptPollInterval.toMillis(), attempts));
        // DefaultGasProvider is a fixed gas price/limit, fine for a local
        // dev chain or a low-traffic testnet. A real deployment would want
        // a provider that reads current network fees instead.
        this.contract = HashAnchor.load(contractAddress, web3j, transactionManager, new DefaultGasProvider());
    }

    public AnchorResult anchor(byte[] docHash) throws Exception {
        TransactionReceipt receipt = contract.anchor(docHash).send();
        return new AnchorResult(receipt.getTransactionHash(), receipt.getBlockNumber().longValueExact());
    }

    public VerifyResult verify(byte[] docHash) throws Exception {
        Tuple2<Boolean, BigInteger> result = contract.verify(docHash).send();
        return new VerifyResult(result.component1(), result.component2().longValueExact());
    }

    /** When block {@code blockNumber} was mined, per the block header's timestamp. */
    public Instant getBlockTimestamp(long blockNumber) throws Exception {
        EthBlock response = web3j.ethGetBlockByNumber(
                        DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber)), false)
                .send();
        if (response.hasError() || response.getBlock() == null) {
            throw new IllegalStateException("No block " + blockNumber + " on this chain");
        }
        return Instant.ofEpochSecond(response.getBlock().getTimestamp().longValueExact());
    }

    /**
     * Finds the transaction that anchored {@code docHash}, for when we know
     * it's anchored (from {@link #verify}) but not which transaction did it.
     * Searches only {@code blockNumber} — the block {@code verify()}
     * reported — for the contract's {@code HashAnchored} event with
     * {@code docHash} as its indexed topic. The contract only emits it once
     * per hash, so there's at most one match.
     *
     * <p>Empty if the node returns no matching log (e.g. a provider that
     * doesn't serve logs that far back); throws if the RPC call itself fails.
     */
    public Optional<String> findAnchorTransactionHash(byte[] docHash, long blockNumber) throws Exception {
        DefaultBlockParameter block = DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber));
        EthFilter filter = new EthFilter(block, block, contractAddress)
                .addSingleTopic(EventEncoder.encode(HashAnchor.HASHANCHORED_EVENT))
                .addSingleTopic(Numeric.toHexString(docHash));
        EthLog response = web3j.ethGetLogs(filter).send();
        if (response.hasError()) {
            throw new IllegalStateException("eth_getLogs failed: " + response.getError().getMessage());
        }
        return response.getLogs().stream()
                .map(result -> ((Log) result.get()).getTransactionHash())
                .findFirst();
    }
}
