package com.hashanchor.domain;

import java.util.UUID;

/**
 * An event referenced a record that doesn't exist. Retrying can't fix that,
 * so the Kafka error handler is configured to send these straight to the
 * dead-letter topic without backoff.
 */
public class RecordNotFoundException extends RuntimeException {

    public RecordNotFoundException(UUID recordId) {
        super("No record with id " + recordId);
    }
}
