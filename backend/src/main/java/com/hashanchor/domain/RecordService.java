package com.hashanchor.domain;

import tools.jackson.databind.ObjectMapper;
import com.hashanchor.persistence.OutboxEventRepository;
import com.hashanchor.persistence.RecordRepository;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Holds the submit-path business logic. Spring creates exactly one instance
 * of this class (a "bean") and hands it, via the constructor, to whatever
 * else asks for a {@code RecordService} — this is dependency injection: we
 * declare what we need as constructor parameters and never call {@code new}
 * ourselves. The repositories below are themselves injected the same way.
 */
@Service
public class RecordService {

    private final RecordRepository recordRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public RecordService(
            RecordRepository recordRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.recordRepository = recordRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Saves the record and its outbox event in one database transaction.
     *
     * <p>{@code @Transactional} wraps this method in a single DB
     * transaction: both {@code save} calls commit together, or (if
     * anything throws) both roll back together. That's what makes the
     * outbox pattern work — it's impossible to persist the record without
     * also queuing the event, or vice versa. No Kafka call happens here;
     * the relay (Phase 3) is what actually publishes the outbox row.
     */
    @Transactional
    public DocumentRecord submit(byte[] documentBytes) {
        String documentHash = DocumentHashes.sha256Hex(documentBytes);

        DocumentRecord record = new DocumentRecord();
        record.setDocumentHash(documentHash);
        record.setStatus(RecordStatus.PENDING);
        record = recordRepository.save(record);

        enqueueSubmittedEvent(record);

        return record;
    }

    /**
     * Sends a record back through the pipeline: resets it to
     * {@code PENDING} and queues a fresh {@code RecordSubmitted} outbox
     * event, atomically — the same guarantee as {@link #submit}. Used by
     * {@link StaleAnchoringRecovery} for records abandoned mid-attempt.
     *
     * <p>{@code save} throws {@code ObjectOptimisticLockingFailureException}
     * if the anchoring consumer updated the row since {@code record} was
     * read, which rolls back the outbox insert too.
     */
    @Transactional
    public void requeueForAnchoring(DocumentRecord record) {
        record.setStatus(RecordStatus.PENDING);
        recordRepository.save(record);
        enqueueSubmittedEvent(record);
    }

    private void enqueueSubmittedEvent(DocumentRecord record) {
        OutboxEvent event = new OutboxEvent();
        event.setRecordId(record.getId());
        event.setEventType("RecordSubmitted");
        event.setPayload(buildPayload(record));
        outboxEventRepository.save(event);
    }

    @Transactional(readOnly = true)
    public Optional<DocumentRecord> findById(UUID id) {
        return recordRepository.findById(id);
    }

    private String buildPayload(DocumentRecord record) {
        try {
            return objectMapper.writeValueAsString(
                    Map.of(
                            "recordId", record.getId().toString(),
                            "documentHash", record.getDocumentHash()));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
