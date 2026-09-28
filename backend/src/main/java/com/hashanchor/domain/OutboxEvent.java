package com.hashanchor.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A row in the transactional outbox. Written in the same DB transaction as
 * the {@link DocumentRecord} it describes, so "record saved" and "event
 * queued for Kafka" can never happen independently. The relay (Phase 3)
 * polls for {@code published = false} rows, publishes them to Kafka, and
 * flips {@code published} — this table is the only writer of Kafka events
 * for the submit path.
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "record_id", nullable = false)
    private UUID recordId;

    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    // Stored as a pre-serialized JSON string; Hibernate writes it straight
    // into the jsonb column rather than double-encoding it.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(nullable = false)
    private boolean published = false;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;
}
