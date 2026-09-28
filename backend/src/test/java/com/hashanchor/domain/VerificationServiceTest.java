package com.hashanchor.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hashanchor.blockchain.AnchorClient;
import com.hashanchor.blockchain.VerifyResult;
import com.hashanchor.persistence.RecordRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VerificationServiceTest {

    private static final byte[] DOCUMENT = "hello".getBytes(StandardCharsets.UTF_8);
    // sha256("hello")
    private static final String DOCUMENT_HASH =
            "0x2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    private AnchorClient anchorClient;
    private RecordRepository recordRepository;
    private VerificationService service;

    @BeforeEach
    void setUp() {
        anchorClient = mock(AnchorClient.class);
        recordRepository = mock(RecordRepository.class);
        service = new VerificationService(anchorClient, recordRepository);
    }

    @Test
    void reportsBlockTimestampTxHashAndRecordForAnAnchoredDocument() throws Exception {
        Instant minedAt = Instant.parse("2026-09-29T10:00:00Z");
        DocumentRecord record = record(RecordStatus.ANCHORED);
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(true, 12));
        when(anchorClient.getBlockTimestamp(12)).thenReturn(minedAt);
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenReturn(Optional.of("0xtx"));
        when(recordRepository.findByDocumentHashOrderByCreatedAtDesc(DOCUMENT_HASH)).thenReturn(List.of(record));

        VerificationResult result = service.verify(DOCUMENT);

        assertThat(result).isEqualTo(new VerificationResult(DOCUMENT_HASH, true, 12L, minedAt, "0xtx", record.getId()));
    }

    @Test
    void reportsNotAnchoredForAnUnknownDocument() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(recordRepository.findByDocumentHashOrderByCreatedAtDesc(anyString())).thenReturn(List.of());

        VerificationResult result = service.verify(DOCUMENT);

        assertThat(result).isEqualTo(new VerificationResult(DOCUMENT_HASH, false, null, null, null, null));
    }

    @Test
    void theChainDecidesEvenWhenTheDatabaseSaysAnchored() throws Exception {
        // e.g. a dev chain that was reset after the record was anchored
        DocumentRecord record = record(RecordStatus.ANCHORED);
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(recordRepository.findByDocumentHashOrderByCreatedAtDesc(DOCUMENT_HASH)).thenReturn(List.of(record));

        VerificationResult result = service.verify(DOCUMENT);

        assertThat(result.anchored()).isFalse();
        assertThat(result.recordId()).isEqualTo(record.getId());
    }

    @Test
    void prefersTheAnchoredRecordOverANewerOne() throws Exception {
        DocumentRecord newerPending = record(RecordStatus.PENDING);
        DocumentRecord olderAnchored = record(RecordStatus.ANCHORED);
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(recordRepository.findByDocumentHashOrderByCreatedAtDesc(DOCUMENT_HASH))
                .thenReturn(List.of(newerPending, olderAnchored));

        assertThat(service.verify(DOCUMENT).recordId()).isEqualTo(olderAnchored.getId());
    }

    @Test
    void stillAnswersFromTheChainWhenTheDatabaseIsDown() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(true, 12));
        when(anchorClient.getBlockTimestamp(12)).thenReturn(Instant.EPOCH);
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenReturn(Optional.of("0xtx"));
        when(recordRepository.findByDocumentHashOrderByCreatedAtDesc(anyString()))
                .thenThrow(new RuntimeException("db down"));

        VerificationResult result = service.verify(DOCUMENT);

        assertThat(result.anchored()).isTrue();
        assertThat(result.recordId()).isNull();
    }

    @Test
    void propagatesChainFailuresInsteadOfAnsweringNotAnchored() throws Exception {
        when(anchorClient.verify(any())).thenThrow(new IOException("rpc down"));

        assertThatThrownBy(() -> service.verify(DOCUMENT)).isInstanceOf(IOException.class);
    }

    private static DocumentRecord record(RecordStatus status) {
        DocumentRecord record = new DocumentRecord();
        record.setId(UUID.randomUUID());
        record.setDocumentHash(DOCUMENT_HASH);
        record.setStatus(status);
        return record;
    }
}
