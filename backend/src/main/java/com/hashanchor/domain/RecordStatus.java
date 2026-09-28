package com.hashanchor.domain;

/**
 * {@code PENDING → ANCHORING → ANCHORED} on the happy path, or
 * {@code ANCHORING → FAILED} once retries are exhausted. {@code ANCHORING}
 * means the consumer has started working on a record and hasn't finished;
 * a record that stays in it for too long was abandoned mid-attempt (for
 * example, the process crashed) and is picked up by
 * {@link StaleAnchoringRecovery}.
 */
public enum RecordStatus {
    PENDING,
    ANCHORING,
    ANCHORED,
    FAILED
}
