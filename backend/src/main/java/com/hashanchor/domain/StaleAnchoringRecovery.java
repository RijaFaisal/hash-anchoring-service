package com.hashanchor.domain;

import com.hashanchor.blockchain.AnchorClient;
import com.hashanchor.blockchain.VerifyResult;
import com.hashanchor.persistence.RecordRepository;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.web3j.utils.Numeric;

/**
 * Finds records stuck in {@code ANCHORING} — the consumer started work on
 * them and then stopped making progress — and gets them moving again.
 *
 * <p>Usually a crash mid-attempt needs no help: the consumer hadn't
 * committed the Kafka offset yet, so the event is redelivered on restart
 * and {@link AnchoringService} resumes from {@code ANCHORING}. This job
 * covers the cases where that doesn't happen — e.g. the offset was
 * committed (the event was dead-lettered) but the process died before
 * marking the record {@code FAILED}, or the event was lost for some reason
 * we didn't foresee.
 *
 * <p>For each stale record it asks the chain first: if the hash is
 * anchored, the record is marked {@code ANCHORED}; otherwise it's reset to
 * {@code PENDING} with a fresh outbox event, so it goes back through the
 * normal pipeline rather than this job sending transactions itself.
 *
 * <p>"Stale" means {@code updated_at} is older than {@code stale-after}.
 * The consumer touches the row at the start of every attempt, and a single
 * attempt is bounded by the receipt timeout, so a record that's actually
 * being worked on never looks stale. If this job and the consumer do race,
 * the {@code @Version} column makes the slower write fail rather than
 * clobber the faster one.
 */
@Component
public class StaleAnchoringRecovery {

    private static final Logger log = LoggerFactory.getLogger(StaleAnchoringRecovery.class);

    private final RecordRepository recordRepository;
    private final RecordService recordService;
    private final AnchoringService anchoringService;
    private final AnchorClient anchorClient;
    private final Duration staleAfter;

    public StaleAnchoringRecovery(
            RecordRepository recordRepository,
            RecordService recordService,
            AnchoringService anchoringService,
            AnchorClient anchorClient,
            @Value("${hashanchor.anchoring.stale-after}") Duration staleAfter) {
        this.recordRepository = recordRepository;
        this.recordService = recordService;
        this.anchoringService = anchoringService;
        this.anchorClient = anchorClient;
        this.staleAfter = staleAfter;
    }

    // The initial delay gives Kafka redelivery the first chance at records
    // a crash left behind, before this job starts second-guessing them.
    @Scheduled(
            initialDelayString = "${hashanchor.anchoring.stale-check-interval}",
            fixedDelayString = "${hashanchor.anchoring.stale-check-interval}")
    public void recoverStaleRecords() {
        Instant cutoff = Instant.now().minus(staleAfter);
        for (DocumentRecord record : recordRepository.findByStatusAndUpdatedAtBefore(RecordStatus.ANCHORING, cutoff)) {
            try {
                recover(record);
            } catch (Exception e) {
                // Left in ANCHORING, so the next run tries again.
                log.warn("Couldn't recover stale ANCHORING record {}; will retry next run", record.getId(), e);
            }
        }
    }

    private void recover(DocumentRecord record) throws Exception {
        byte[] docHash = Numeric.hexStringToByteArray(record.getDocumentHash());
        VerifyResult onChain = anchorClient.verify(docHash);
        if (onChain.anchored()) {
            anchoringService.markAnchoredFromChain(record, docHash, onChain.blockNumber());
            log.info("Stale ANCHORING record {} is anchored on-chain at block {}; marked ANCHORED",
                    record.getId(), onChain.blockNumber());
        } else {
            recordService.requeueForAnchoring(record);
            log.info("Stale ANCHORING record {} isn't on-chain; reset to PENDING and re-queued", record.getId());
        }
    }
}
