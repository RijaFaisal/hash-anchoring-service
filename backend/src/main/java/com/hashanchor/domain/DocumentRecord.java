package com.hashanchor.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * A tracked submission: a document's hash plus its anchoring status.
 *
 * <p>Named {@code DocumentRecord} rather than {@code Record} to avoid
 * colliding with {@code java.lang.Record}, the language feature backing
 * Java's {@code record} keyword (used elsewhere in this codebase for DTOs).
 */
@Entity
@Table(name = "records")
@Getter
@Setter
public class DocumentRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "document_hash", nullable = false, length = 66)
    private String documentHash;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private RecordStatus status;

    @Column(name = "tx_hash", length = 66)
    private String txHash;

    @Column(name = "block_number")
    private Long blockNumber;

    /** How many times the anchoring consumer has started work on this record. */
    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    // Optimistic locking: Hibernate adds "AND version = ?" to every UPDATE
    // and bumps the value. If another writer changed the row since we read
    // it, the update matches zero rows and Hibernate throws
    // ObjectOptimisticLockingFailureException instead of overwriting.
    @Version
    @Column(nullable = false)
    private long version;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
