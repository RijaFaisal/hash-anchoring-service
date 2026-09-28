package com.hashanchor.messaging;

import com.hashanchor.domain.AnchoringService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes {@code RecordSubmitted} events and hands each one to
 * {@link AnchoringService}.
 *
 * <p>{@code @KafkaListener} makes Spring Kafka start a background consumer
 * (in the {@code hash-anchor-anchoring} group, from application.yml) that
 * polls {@code records.submitted} and calls this method once per message.
 * If the method returns, the message's offset is committed and it won't be
 * delivered to this group again. If it throws, the error handler in
 * {@link com.hashanchor.config.KafkaConsumerConfig} decides what happens
 * next: back off and redeliver the same message, or, once retries run out,
 * publish it to {@code records.submitted.DLT} and move on.
 *
 * <p>Deliberately thin — parsing the message and delegating. Everything that
 * makes redelivery safe lives in {@link AnchoringService}.
 */
@Component
public class AnchoringConsumer {

    private final AnchoringService anchoringService;
    private final ObjectMapper objectMapper;

    public AnchoringConsumer(AnchoringService anchoringService, ObjectMapper objectMapper) {
        this.anchoringService = anchoringService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.RECORDS_SUBMITTED)
    public void onRecordSubmitted(ConsumerRecord<String, String> message) throws Exception {
        RecordSubmittedEvent event = objectMapper.readValue(message.value(), RecordSubmittedEvent.class);
        anchoringService.anchor(event.recordId());
    }

    /**
     * The outbox payload written by {@code RecordService}. Only
     * {@code recordId} is used: the hash to anchor is read from the record
     * itself, so the database stays the one place it comes from.
     */
    record RecordSubmittedEvent(UUID recordId, String documentHash) {}
}
