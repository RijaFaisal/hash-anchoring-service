package com.hashanchor.config;

import com.hashanchor.domain.AnchoringService;
import com.hashanchor.domain.RecordNotFoundException;
import com.hashanchor.messaging.Topics;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import tools.jackson.core.JacksonException;

/**
 * Retry, backoff and dead-lettering for {@code @KafkaListener} methods.
 *
 * <p>Spring Boot auto-configures the listener container factory (the thing
 * that creates a consumer for each {@code @KafkaListener}); if it finds a
 * {@code CommonErrorHandler} bean, like {@link #anchoringErrorHandler}
 * below, it plugs it into that factory. No factory of our own needed.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // NewTopic beans are picked up by Spring Boot's KafkaAdmin, which
    // creates any that don't exist yet at startup (and leaves existing
    // ones alone). One partition each matches the single-broker dev setup.
    @Bean
    public NewTopic recordsSubmittedTopic() {
        return TopicBuilder.name(Topics.RECORDS_SUBMITTED).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic recordsSubmittedDltTopic() {
        return TopicBuilder.name(Topics.RECORDS_SUBMITTED_DLT).partitions(1).replicas(1).build();
    }

    /**
     * When the listener throws, {@link DefaultErrorHandler} sleeps for the
     * next backoff interval, then seeks the consumer back so the same
     * message is redelivered — 1s, 2s, 4s, 8s: four retries, five attempts
     * in all. After the last one fails it calls the recoverer below instead
     * of retrying again, then commits the offset so the partition moves on.
     *
     * <p>The backoff sleeps happen on the consumer thread, so their total
     * plus the attempts themselves must stay under
     * {@code max.poll.interval.ms} (5 minutes by default) or the broker
     * assumes this consumer died. 15s of backoff plus five attempts, each
     * capped by the 30s receipt timeout, fits comfortably.
     */
    @Bean
    public DefaultErrorHandler anchoringErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate, AnchoringService anchoringService) {
        // Partition -1 lets the producer pick the DLT partition from the
        // key, instead of assuming the DLT has as many partitions as the
        // source topic.
        var deadLetterPublisher = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (message, e) -> new TopicPartition(Topics.RECORDS_SUBMITTED_DLT, -1));

        var backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        var errorHandler = new DefaultErrorHandler(
                (message, e) -> {
                    // DLT first, then FAILED. If marking FAILED throws,
                    // the offset isn't committed and the whole thing is
                    // redelivered — possibly a duplicate on the DLT, but
                    // never a FAILED record with no DLT entry.
                    deadLetterPublisher.accept(message, e);
                    Throwable cause = unwrap(e);
                    parseRecordId((String) message.key())
                            .ifPresent(recordId -> anchoringService.markFailed(recordId, cause));
                },
                backOff);

        // Retrying can't fix these; skip straight to the recoverer.
        errorHandler.addNotRetryableExceptions(RecordNotFoundException.class, JacksonException.class);
        return errorHandler;
    }

    // The listener's exception reaches the error handler wrapped in a
    // ListenerExecutionFailedException; the useful message is the cause's.
    private static Throwable unwrap(Exception e) {
        return e instanceof ListenerExecutionFailedException && e.getCause() != null ? e.getCause() : e;
    }

    private static Optional<UUID> parseRecordId(String key) {
        try {
            return Optional.of(UUID.fromString(key));
        } catch (RuntimeException e) {
            log.warn("Dead-lettered message has no usable record id key ({}); no record to mark FAILED", key);
            return Optional.empty();
        }
    }
}
