package com.hashanchor.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
