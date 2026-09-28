package com.hashanchor.messaging;

import com.hashanchor.domain.OutboxEvent;
import com.hashanchor.persistence.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes unpublished {@link OutboxEvent} rows to Kafka and marks them
 * published. This is the second half of the transactional outbox pattern:
 * {@code RecordService.submit()} guarantees a record is never saved without
 * a matching outbox row; this class guarantees that row eventually reaches
 * Kafka, by retrying on every scheduled run until it succeeds.
 *
 * <p><b>Why consumers must be idempotent.</b> Publishing to Kafka and
 * marking the row published locally are two separate operations that
 * cannot be made atomic — there's no distributed transaction spanning
 * Postgres and Kafka here. If this process crashes (or the DB write fails)
 * in the gap between "Kafka acked the send" and "the row is marked
 * published", the row is still {@code published = false} in Postgres. The
 * next scheduled run sees that same row as unpublished and sends it to
 * Kafka again — so the same {@code RecordSubmitted} event can appear on the
 * topic twice. This makes delivery to {@code records.submitted}
 * at-least-once, never exactly-once. Every consumer of this topic
 * ({@link AnchoringConsumer}) must tolerate processing the same
 * event twice without double-anchoring — checking current status before
 * acting, not assuming each message is new.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // fixedDelay: the next run starts this long after the previous one
    // *finishes*, so runs never overlap even if a batch is slow.
    @Scheduled(fixedDelayString = "${hashanchor.outbox.poll-interval}")
    public void relayUnpublishedEvents() {
        List<OutboxEvent> unpublished = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();
        for (OutboxEvent event : unpublished) {
            publishAndMark(event);
        }
    }

    private void publishAndMark(OutboxEvent event) {
        SendResult<String, String> result;
        try {
            // .get() blocks this scheduled-task thread until Kafka acks (or
            // the send fails) — we need to know which happened before
            // deciding whether to mark the row published, so there's no
            // benefit to treating the send as fire-and-forget here.
            result = kafkaTemplate.send(Topics.RECORDS_SUBMITTED, event.getRecordId().toString(), event.getPayload()).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Failed to publish outbox event {} to Kafka; will retry on the next relay run", event.getId(), e);
            return;
        }

        log.info(
                "Published outbox event {} to {}-{}@{}",
                event.getId(),
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        try {
            event.setPublished(true);
            event.setPublishedAt(Instant.now());
            outboxEventRepository.save(event);
        } catch (Exception e) {
            // The message is already durably on Kafka — it can't be
            // unsent. Leaving `published = false` here means this exact
            // event gets published again on the next run: a duplicate on
            // the topic, not a lost event. See the class-level note on why
            // consumers must be idempotent.
            log.error(
                    "Published outbox event {} to Kafka but failed to mark it published locally; "
                            + "it will be re-published on the next run",
                    event.getId(),
                    e);
        }
    }
}
