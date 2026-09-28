package com.hashanchor.domain;

import com.hashanchor.blockchain.AnchorClient;
import com.hashanchor.blockchain.VerifyResult;
import com.hashanchor.persistence.RecordRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.web3j.utils.Numeric;

/**
 * Answers "was this exact document anchored, and when?" by asking the
 * contract, not our database. Records can be missing, stale, or (after a
 * chain reset in dev) flat-out wrong; the chain is the only thing a
 * verifier should have to trust. The database is consulted afterwards only
 * to attach our record id, and a database failure doesn't fail the
 * verification.
 */
@Service
public class VerificationService {

    private static final Logger log = LoggerFactory.getLogger(VerificationService.class);

    private final AnchorClient anchorClient;
    private final RecordRepository recordRepository;

    public VerificationService(AnchorClient anchorClient, RecordRepository recordRepository) {
        this.anchorClient = anchorClient;
        this.recordRepository = recordRepository;
    }

    public VerificationResult verify(byte[] documentBytes) throws Exception {
        String documentHash = DocumentHashes.sha256Hex(documentBytes);
        byte[] docHash = Numeric.hexStringToByteArray(documentHash);

        VerifyResult onChain = anchorClient.verify(docHash);
        UUID recordId = findRecordId(documentHash);
        if (!onChain.anchored()) {
            return new VerificationResult(documentHash, false, null, null, null, recordId);
        }

        long blockNumber = onChain.blockNumber();
        Instant anchoredAt = anchorClient.getBlockTimestamp(blockNumber);
        String txHash = anchorClient.findAnchorTransactionHash(docHash, blockNumber).orElse(null);
        return new VerificationResult(documentHash, true, blockNumber, anchoredAt, txHash, recordId);
    }

    /**
     * Our record for this hash, if we have one. The same document can be
     * submitted more than once, so prefer an ANCHORED record, then the
     * newest.
     */
    private UUID findRecordId(String documentHash) {
        try {
            List<DocumentRecord> records = recordRepository.findByDocumentHashOrderByCreatedAtDesc(documentHash);
            return records.stream()
                    .filter(r -> r.getStatus() == RecordStatus.ANCHORED)
                    .findFirst()
                    .or(() -> records.stream().findFirst())
                    .map(DocumentRecord::getId)
                    .orElse(null);
        } catch (Exception e) {
            log.warn("Record lookup failed while verifying {}; answering from the chain alone", documentHash, e);
            return null;
        }
    }
}
