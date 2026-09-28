package com.hashanchor.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hashanchor.blockchain.AnchorClient;
import com.hashanchor.blockchain.AnchorResult;
import com.hashanchor.blockchain.VerifyResult;
import com.hashanchor.persistence.RecordRepository;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.exceptions.TransactionException;

/**
 * Plain unit test — no Spring context, no chain, no database. The
 * repository and {@link AnchorClient} are Mockito mocks, so each test can
 * script exactly what the chain "says" and check what the service does
 * about it.
 */
class AnchoringServiceTest {

    private static final String DOC_HASH = "0x" + "ab".repeat(32);

    private RecordRepository recordRepository;
    private AnchorClient anchorClient;
    private AnchoringService service;
    private DocumentRecord record;

    @BeforeEach
    void setUp() {
        recordRepository = mock(RecordRepository.class);
        anchorClient = mock(AnchorClient.class);
        service = new AnchoringService(recordRepository, anchorClient);

        record = new DocumentRecord();
        record.setId(UUID.randomUUID());
        record.setDocumentHash(DOC_HASH);
        record.setStatus(RecordStatus.PENDING);
        when(recordRepository.findById(record.getId())).thenReturn(Optional.of(record));
        // save() returns its argument, like a real repository returning the
        // managed copy.
        when(recordRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void anchorsAndStoresTxHashAndBlockNumber() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(anchorClient.anchor(any())).thenReturn(new AnchorResult("0xtx", 42));

        service.anchor(record.getId());

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(record.getTxHash()).isEqualTo("0xtx");
        assertThat(record.getBlockNumber()).isEqualTo(42);
        assertThat(record.getAttempts()).isEqualTo(1);
    }

    @Test
    void skipsARecordThatIsAlreadyAnchoredLocally() throws Exception {
        record.setStatus(RecordStatus.ANCHORED);

        service.anchor(record.getId());

        verify(anchorClient, never()).verify(any());
        verify(anchorClient, never()).anchor(any());
        assertThat(record.getAttempts()).isZero();
    }

    @Test
    void skipsAFailedRecord() throws Exception {
        record.setStatus(RecordStatus.FAILED);

        service.anchor(record.getId());

        verify(anchorClient, never()).anchor(any());
        assertThat(record.getStatus()).isEqualTo(RecordStatus.FAILED);
    }

    @Test
    void marksAnchoredWithoutSendingATransactionWhenVerifyShowsItOnChain() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(true, 7));
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenReturn(Optional.of("0xfromlog"));

        service.anchor(record.getId());

        verify(anchorClient, never()).anchor(any());
        verify(anchorClient).findAnchorTransactionHash(any(), eq(7L));
        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(record.getBlockNumber()).isEqualTo(7);
        assertThat(record.getTxHash()).isEqualTo("0xfromlog");
    }

    @Test
    void stillMarksAnchoredWhenNoEventLogIsFound() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(true, 7));
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenReturn(Optional.empty());

        service.anchor(record.getId());

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(record.getTxHash()).isNull();
    }

    @Test
    void retriesRatherThanDropTheTxHashWhenTheLogLookupFails() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(true, 7));
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenThrow(new IOException("rpc down"));

        assertThatThrownBy(() -> service.anchor(record.getId())).isInstanceOf(IOException.class);

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORING);
    }

    @Test
    void resumesARecordLeftInAnchoringByACrashedAttempt() throws Exception {
        record.setStatus(RecordStatus.ANCHORING);
        record.setAttempts(1);
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(anchorClient.anchor(any())).thenReturn(new AnchorResult("0xtx", 42));

        service.anchor(record.getId());

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(record.getAttempts()).isEqualTo(2);
    }

    @Test
    void treatsAFailedAnchorAsSuccessIfVerifyNowShowsItAnchored() throws Exception {
        // First verify: not there yet. anchor() then fails (a revert, a
        // receipt timeout — doesn't matter which). Second verify: there.
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0), new VerifyResult(true, 9));
        when(anchorClient.anchor(any())).thenThrow(new TransactionException("receipt timeout"));
        when(anchorClient.findAnchorTransactionHash(any(), anyLong())).thenReturn(Optional.of("0xfromlog"));

        service.anchor(record.getId());

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(record.getBlockNumber()).isEqualTo(9);
        assertThat(record.getTxHash()).isEqualTo("0xfromlog");
    }

    @Test
    void rethrowsAndRecordsTheErrorWhenAnchorFailsAndVerifyAgrees() throws Exception {
        when(anchorClient.verify(any())).thenReturn(new VerifyResult(false, 0));
        when(anchorClient.anchor(any())).thenThrow(new TransactionException("reverted"));

        assertThatThrownBy(() -> service.anchor(record.getId())).isInstanceOf(TransactionException.class);

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORING);
        assertThat(record.getLastError()).contains("reverted");
    }

    @Test
    void throwsNotFoundForAnUnknownRecord() {
        UUID unknown = UUID.randomUUID();
        when(recordRepository.findById(unknown)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.anchor(unknown)).isInstanceOf(RecordNotFoundException.class);
    }

    @Test
    void markFailedLeavesAnAnchoredRecordAlone() {
        record.setStatus(RecordStatus.ANCHORED);

        service.markFailed(record.getId(), new RuntimeException("boom"));

        assertThat(record.getStatus()).isEqualTo(RecordStatus.ANCHORED);
    }

    @Test
    void markFailedSetsFailedWithTheError() {
        record.setStatus(RecordStatus.ANCHORING);

        service.markFailed(record.getId(), new RuntimeException("boom"));

        assertThat(record.getStatus()).isEqualTo(RecordStatus.FAILED);
        assertThat(record.getLastError()).isEqualTo("RuntimeException: boom");
    }
}
