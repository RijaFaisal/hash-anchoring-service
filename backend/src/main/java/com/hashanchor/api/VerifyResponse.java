package com.hashanchor.api;

import com.hashanchor.domain.VerificationResult;
import java.time.Instant;
import java.util.UUID;

/**
 * Body of {@code POST /api/verify}. For an unknown document: {@code anchored}
 * is false and the chain fields are null ({@code recordId} may still be set
 * if we've accepted a submission for it that hasn't been anchored yet).
 */
public record VerifyResponse(
        String documentHash,
        boolean anchored,
        Long blockNumber,
        Instant anchoredAt,
        String txHash,
        UUID recordId) {

    public static VerifyResponse from(VerificationResult result) {
        return new VerifyResponse(
                result.documentHash(),
                result.anchored(),
                result.blockNumber(),
                result.anchoredAt(),
                result.txHash(),
                result.recordId());
    }
}
