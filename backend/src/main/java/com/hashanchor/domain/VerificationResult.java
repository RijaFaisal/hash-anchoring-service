package com.hashanchor.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * What the chain says about a document. {@code blockNumber},
 * {@code anchoredAt} and {@code txHash} are null unless {@code anchored};
 * {@code txHash} can also be null if the anchoring event log couldn't be
 * found. {@code recordId} is our local submission for this hash, if any —
 * informational only, it never affects {@code anchored}.
 */
public record VerificationResult(
        String documentHash,
        boolean anchored,
        Long blockNumber,
        Instant anchoredAt,
        String txHash,
        UUID recordId) {}
