package com.hashanchor.domain;

import com.hashanchor.blockchain.AnchorClient;
import com.hashanchor.blockchain.AnchorResult;
import com.hashanchor.blockchain.VerifyResult;
import com.hashanchor.persistence.RecordRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.web3j.utils.Numeric;

/**
 * Anchors one record on-chain, safely under redelivery. Called once per
 * Kafka delivery attempt by {@code AnchoringConsumer}; retries, backoff and
 * the dead-letter topic are all handled by Spring Kafka around it (see
 * {@code KafkaConsumerConfig}), so this method only needs to do one attempt
 * and either return (done) or throw (try again later).
 *
 * <p><b>Idempotency.</b> Every step is safe to repeat:
 *
 * <ul>
 *   <li>{@code ANCHORED}/{@code FAILED} records are skipped outright.
 *   <li>{@code verify()} runs before {@code anchor()}, so a record whose
 *       earlier attempt already landed on-chain is just marked
 *       {@code ANCHORED} — no second transaction.
 *   <li>If {@code anchor()} fails for any reason (revert, receipt timeout,
 *       RPC error), {@code verify()} decides whether it really failed. We
 *       never look at the revert string; the chain is the source of truth.
 * </ul>
 *
 * <p><b>No DB transaction spans the chain calls.</b> Each {@code save} is
 * its own short transaction (Spring Data's repository methods are
 * transactional on their own). Holding a Postgres transaction open for the
 * seconds an on-chain write takes would pin a connection and hold row
 * locks for no benefit — and it couldn't roll the chain back anyway.
 */
@Service
public class AnchoringService {

    private static final Logger log = LoggerFactory.getLogger(AnchoringService.class);
    private static final int MAX_ERROR_LENGTH = 2000;

    private final RecordRepository recordRepository;
    private final AnchorClient anchorClient;

    public AnchoringService(RecordRepository recordRepository, AnchorClient anchorClient) {
        this.recordRepository = recordRepository;
        this.anchorClient = anchorClient;
    }

    /**
     * Makes one anchoring attempt. Returns normally when the record is
     * {@code ANCHORED} (or was already done); throws when this attempt
     * failed and a retry could succeed.
     */
    public void anchor(UUID recordId) throws Exception {
        DocumentRecord record =
                recordRepository.findById(recordId).orElseThrow(() -> new RecordNotFoundException(recordId));

        if (record.getStatus() == RecordStatus.ANCHORED || record.getStatus() == RecordStatus.FAILED) {
            log.info("Record {} is already {}; skipping", recordId, record.getStatus());
            return;
        }

        // PENDING, or ANCHORING left over from an attempt that crashed or
        // threw — either way, start (or resume) from the top.
        record.setStatus(RecordStatus.ANCHORING);
        record.setAttempts(record.getAttempts() + 1);
        record = recordRepository.save(record);
        log.info("Anchoring record {} (attempt {})", recordId, record.getAttempts());

        byte[] docHash = Numeric.hexStringToByteArray(record.getDocumentHash());
        try {
            VerifyResult onChain = anchorClient.verify(docHash);
            if (onChain.anchored()) {
                log.info("Record {} is already anchored on-chain at block {}; no transaction sent",
                        recordId, onChain.blockNumber());
                markAnchoredFromChain(record, docHash, onChain.blockNumber());
                return;
            }

            AnchorResult result;
            try {
                result = anchorClient.anchor(docHash);
            } catch (Exception anchorFailure) {
                VerifyResult afterFailure = anchorClient.verify(docHash);
                if (afterFailure.anchored()) {
                    log.info("anchor() for record {} failed ({}), but verify() shows it anchored at block {}; "
                            + "treating as success", recordId, anchorFailure.getMessage(), afterFailure.blockNumber());
                    markAnchoredFromChain(record, docHash, afterFailure.blockNumber());
                    return;
                }
                throw anchorFailure;
            }

            markAnchored(record, result.transactionHash(), result.blockNumber());
            log.info("Anchored record {} in tx {} at block {}", recordId, result.transactionHash(), result.blockNumber());
        } catch (Exception e) {
            recordLastError(record, e);
            throw e;
        }
    }

    /**
     * Called once retries are exhausted (after the event has gone to the
     * dead-letter topic). Leaves an {@code ANCHORED} record alone — if it
     * somehow got anchored in the meantime, that wins.
     */
    public void markFailed(UUID recordId, Throwable cause) {
        recordRepository.findById(recordId).ifPresentOrElse(
                record -> {
                    if (record.getStatus() == RecordStatus.ANCHORED) {
                        return;
                    }
                    record.setStatus(RecordStatus.FAILED);
                    record.setLastError(describe(cause));
                    recordRepository.save(record);
                    log.warn("Record {} marked FAILED after {} attempts: {}",
                            recordId, record.getAttempts(), record.getLastError());
                },
                () -> log.warn("Can't mark record {} FAILED: no such record", recordId));
    }

    /**
     * Marks a record {@code ANCHORED} when we learned it's on-chain from
     * {@code verify()}, which only returns the block number. The
     * transaction hash is recovered from that block's {@code HashAnchored}
     * event. If the RPC call fails this throws, so the attempt is retried
     * rather than the record being saved without a {@code tx_hash} it could
     * have had.
     */
    void markAnchoredFromChain(DocumentRecord record, byte[] docHash, long blockNumber) throws Exception {
        String txHash = anchorClient.findAnchorTransactionHash(docHash, blockNumber).orElse(null);
        if (txHash == null) {
            log.warn("Record {} is anchored at block {} but no HashAnchored event was found there; "
                    + "saving without tx_hash", record.getId(), blockNumber);
        }
        markAnchored(record, txHash, blockNumber);
    }

    private void markAnchored(DocumentRecord record, String txHash, long blockNumber) {
        record.setStatus(RecordStatus.ANCHORED);
        record.setTxHash(txHash);
        record.setBlockNumber(blockNumber);
        recordRepository.save(record);
    }

    private void recordLastError(DocumentRecord record, Exception e) {
        try {
            record.setLastError(describe(e));
            recordRepository.save(record);
        } catch (Exception saveFailure) {
            // Best effort: the original error is what gets rethrown and
            // retried; failing to record it shouldn't replace it.
            log.warn("Couldn't save last_error for record {}", record.getId(), saveFailure);
        }
    }

    private static String describe(Throwable t) {
        String text = t.getClass().getSimpleName() + ": " + t.getMessage();
        return text.length() > MAX_ERROR_LENGTH ? text.substring(0, MAX_ERROR_LENGTH) : text;
    }
}
