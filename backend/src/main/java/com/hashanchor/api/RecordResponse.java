package com.hashanchor.api;

import com.hashanchor.domain.DocumentRecord;
import com.hashanchor.domain.RecordStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * A Java {@code record} (language feature) used as an immutable DTO — not
 * to be confused with {@link DocumentRecord}, the JPA entity. The compiler
 * generates the constructor, accessors, {@code equals}/{@code hashCode}, and
 * {@code toString} from the field list below.
 */
public record RecordResponse(
        UUID id,
        String documentHash,
        RecordStatus status,
        String txHash,
        Long blockNumber,
        Instant createdAt,
        Instant updatedAt) {

    public static RecordResponse from(DocumentRecord record) {
        return new RecordResponse(
                record.getId(),
                record.getDocumentHash(),
                record.getStatus(),
                record.getTxHash(),
                record.getBlockNumber(),
                record.getCreatedAt(),
                record.getUpdatedAt());
    }
}
