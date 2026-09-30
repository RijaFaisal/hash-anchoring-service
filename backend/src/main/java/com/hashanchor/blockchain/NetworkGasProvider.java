package com.hashanchor.blockchain;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthGasPrice;
import org.web3j.tx.gas.ContractGasProvider;
import org.web3j.utils.Convert;

/**
 * Prices each transaction at the node's current {@code eth_gasPrice},
 * capped at {@code maxGasPriceGwei}, with a fixed gas limit.
 *
 * <p>Replaces web3j's {@code DefaultGasProvider}, whose fixed 4.1 gwei is
 * below Polygon's 25 gwei minimum tip (so Amoy rejects the transaction) and
 * whose 9,000,000 gas limit makes the node require the sender to hold
 * limit × price up front — about 0.45 POL at 50 gwei, just to send a
 * transaction that actually uses ~46,500 gas.
 *
 * <p>The cap exists because Amoy's {@code eth_gasPrice} is skewed by a few
 * transactions paying 500 gwei, while nearly everything lands at ~30. If
 * the cap is ever below what the network needs, the transaction waits
 * unmined, the receipt times out, and the normal verify-then-retry path
 * takes over — so a too-low cap costs time, never correctness.
 *
 * <p>web3j calls {@link #getGasPrice(String)} once per transaction, so each
 * {@code anchor()} is priced at send time rather than at startup.
 */
class NetworkGasProvider implements ContractGasProvider {

    private final Web3j web3j;
    private final BigInteger gasLimit;
    private final BigInteger maxGasPrice;

    NetworkGasProvider(Web3j web3j, long gasLimit, Long maxGasPriceGwei) {
        this.web3j = web3j;
        this.gasLimit = BigInteger.valueOf(gasLimit);
        this.maxGasPrice = maxGasPriceGwei == null
                ? null
                : Convert.toWei(maxGasPriceGwei.toString(), Convert.Unit.GWEI).toBigIntegerExact();
    }

    @Override
    public BigInteger getGasPrice(String contractFunc) {
        return getGasPrice();
    }

    @Override
    public BigInteger getGasPrice() {
        EthGasPrice response;
        try {
            response = web3j.ethGasPrice().send();
        } catch (IOException e) {
            // The interface doesn't let us throw IOException; this still
            // surfaces from contract.anchor(...).send() as a failed attempt.
            throw new UncheckedIOException("eth_gasPrice failed", e);
        }
        if (response.hasError()) {
            throw new IllegalStateException("eth_gasPrice failed: " + response.getError().getMessage());
        }
        BigInteger suggested = response.getGasPrice();
        return maxGasPrice == null ? suggested : suggested.min(maxGasPrice);
    }

    @Override
    public BigInteger getGasLimit(String contractFunc) {
        return gasLimit;
    }

    @Override
    public BigInteger getGasLimit() {
        return gasLimit;
    }
}
