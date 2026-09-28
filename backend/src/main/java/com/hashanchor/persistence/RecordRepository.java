package com.hashanchor.persistence;

import com.hashanchor.domain.DocumentRecord;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data generates the implementation of this interface at startup —
 * {@code save}, {@code findById}, etc. come from {@link JpaRepository} with
 * no code of our own required.
 */
public interface RecordRepository extends JpaRepository<DocumentRecord, UUID> {}
