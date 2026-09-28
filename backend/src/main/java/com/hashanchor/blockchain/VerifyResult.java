package com.hashanchor.blockchain;

/** The outcome of {@link AnchorClient#verify(byte[])}: mirrors the contract's own (bool, uint64) return. */
public record VerifyResult(boolean anchored, long blockNumber) {}
