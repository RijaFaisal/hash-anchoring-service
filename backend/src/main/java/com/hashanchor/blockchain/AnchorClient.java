package com.hashanchor.blockchain;

import com.hashanchor.blockchain.generated.HashAnchor;
import java.math.BigInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.tuples.generated.Tuple2;
import org.web3j.tx.gas.DefaultGasProvider;

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
 * <p>No retry or "already anchored" handling lives here — that's the
 * anchoring consumer's job (a later phase). This class only wraps the two
 * raw contract calls.
 */
@Component
public class AnchorClient {

    private final HashAnchor contract;

    public AnchorClient(
            Web3j web3j,
            Credentials credentials,
            @Value("${hashanchor.blockchain.contract-address}") String contractAddress) {
        // DefaultGasProvider is a fixed gas price/limit, fine for a local
        // dev chain or a low-traffic testnet. A real deployment would want
        // a provider that reads current network fees instead.
        this.contract = HashAnchor.load(contractAddress, web3j, credentials, new DefaultGasProvider());
    }

    public AnchorResult anchor(byte[] docHash) throws Exception {
        TransactionReceipt receipt = contract.anchor(docHash).send();
        return new AnchorResult(receipt.getTransactionHash(), receipt.getBlockNumber().longValueExact());
    }

    public VerifyResult verify(byte[] docHash) throws Exception {
        Tuple2<Boolean, BigInteger> result = contract.verify(docHash).send();
        return new VerifyResult(result.component1(), result.component2().longValueExact());
    }
}
