package com.hashanchor.messaging;

/** Kafka topic names, in one place so producers and consumers can't drift apart. */
public final class Topics {

    public static final String RECORDS_SUBMITTED = "records.submitted";
    public static final String RECORDS_SUBMITTED_DLT = "records.submitted.DLT";

    private Topics() {}
}
