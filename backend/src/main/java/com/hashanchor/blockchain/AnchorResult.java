package com.hashanchor.blockchain;

/** The outcome of a successful {@link AnchorClient#anchor(byte[])} call. */
public record AnchorResult(String transactionHash, long blockNumber) {}
