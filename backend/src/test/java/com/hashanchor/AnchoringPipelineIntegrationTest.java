package com.hashanchor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.hashanchor.api.RecordResponse;
import com.hashanchor.api.VerifyResponse;
import com.hashanchor.domain.DocumentHashes;
import com.hashanchor.domain.DocumentRecord;
import com.hashanchor.domain.OutboxEvent;
import com.hashanchor.domain.RecordStatus;
import com.hashanchor.messaging.Topics;
import com.hashanchor.persistence.OutboxEventRepository;
import com.hashanchor.persistence.RecordRepository;
import com.hashanchor.support.AbstractIntegrationTest;
import com.hashanchor.support.InfrastructureContainers;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;
import org.web3j.protocol.Web3j;

/**
 * The whole pipeline, end to end, with nothing mocked: real HTTP requests
 * to the running app → Postgres (outbox) → the scheduled relay → Kafka →
 * the anchoring consumer → a Hardhat chain, and back out through
 * {@code POST /api/verify}. Everything runs in Testcontainers (see
 * {@link AbstractIntegrationTest}), so the only prerequisite is Docker.
 *
 * <p>The base class's {@code webEnvironment = RANDOM_PORT} starts the real
 * embedded web server on a free port (injected via {@code @LocalServerPort}),
 * rather than the default mock servlet environment — so these requests go
 * through actual HTTP, multipart parsing included.
 *
 * <p>The pipeline is asynchronous, so tests poll for the outcome with
 * Awaitility ({@code await().until(...)}) instead of sleeping a fixed time.
 */
class AnchoringPipelineIntegrationTest extends AbstractIntegrationTest {

    private static final Duration PIPELINE_TIMEOUT = Duration.ofSeconds(30);

    @LocalServerPort
    private int port;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroup;

    @Autowired
    private RecordRepository recordRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Web3j web3j;

    private RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.create("http://localhost:" + port);
    }

    @Test
    void submittedDocumentIsAnchoredOnChainAndVerifies() {
        byte[] document = uniqueDocument();

        ResponseEntity<RecordResponse> submitted = submit(document);
        assertThat(submitted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(submitted.getBody().status()).isEqualTo(RecordStatus.PENDING);
        assertThat(submitted.getBody().documentHash()).isEqualTo(DocumentHashes.sha256Hex(document));

        RecordResponse anchored = awaitAnchored(submitted.getBody().id());
        assertThat(anchored.txHash()).matches("0x[0-9a-f]{64}");
        assertThat(anchored.blockNumber()).isPositive();
        assertThat(anchored.attempts()).isEqualTo(1);
        assertThat(anchored.lastError()).isNull();

        VerifyResponse verified = verify(document);
        assertThat(verified.anchored()).isTrue();
        assertThat(verified.documentHash()).isEqualTo(anchored.documentHash());
        assertThat(verified.blockNumber()).isEqualTo(anchored.blockNumber());
        assertThat(verified.txHash()).isEqualTo(anchored.txHash());
        assertThat(verified.recordId()).isEqualTo(anchored.id());
        assertThat(verified.anchoredAt()).isNotNull();
    }

    @Test
    void tamperedDocumentFailsVerification() {
        byte[] original = uniqueDocument();
        submitAndAwaitAnchored(original);

        byte[] tampered = original.clone();
        tampered[0] ^= 1; // flip one bit of one byte

        VerifyResponse verified = verify(tampered);
        assertThat(verified.anchored()).isFalse();
        assertThat(verified.documentHash()).isNotEqualTo(DocumentHashes.sha256Hex(original));
        assertThat(verified.blockNumber()).isNull();
        assertThat(verified.anchoredAt()).isNull();
        assertThat(verified.txHash()).isNull();
        assertThat(verified.recordId()).isNull();

        // ...while the untouched original still verifies.
        assertThat(verify(original).anchored()).isTrue();
    }

    @Test
    void neverSubmittedDocumentIsNotAnchored() {
        VerifyResponse verified = verify(uniqueDocument());

        assertThat(verified.anchored()).isFalse();
        assertThat(verified.recordId()).isNull();
    }

    /**
     * The relay's at-least-once case: the same RecordSubmitted event
     * arrives again after the record is already ANCHORED. It must be
     * skipped — no new transaction, record untouched.
     */
    @Test
    void duplicateEventForAnAnchoredRecordIsSkipped() throws Exception {
        RecordResponse anchored = submitAndAwaitAnchored(uniqueDocument());
        BigInteger chainHeadBefore = chainHead();

        publishDuplicateEvent(anchored.id());
        awaitConsumerCaughtUp();

        DocumentRecord after = recordRepository.findById(anchored.id()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(RecordStatus.ANCHORED);
        assertThat(after.getAttempts()).isEqualTo(anchored.attempts());
        assertThat(after.getTxHash()).isEqualTo(anchored.txHash());
        assertThat(after.getBlockNumber()).isEqualTo(anchored.blockNumber());
        // Hardhat mines one block per transaction: an unchanged head means
        // no transaction was sent.
        assertThat(chainHead()).isEqualTo(chainHeadBefore);
    }

    /**
     * The nastier duplicate: the hash is on-chain, but our DB never
     * recorded it (simulated by resetting the row to PENDING — as if the
     * process died between the receipt and the DB update). The redelivered
     * event must be resolved by verify() — same block, tx_hash recovered
     * from the event log — without sending a second anchor().
     */
    @Test
    void duplicateEventAfterALostDatabaseWriteIsResolvedFromTheChain() throws Exception {
        RecordResponse anchored = submitAndAwaitAnchored(uniqueDocument());
        DocumentRecord record = recordRepository.findById(anchored.id()).orElseThrow();
        record.setStatus(RecordStatus.PENDING);
        record.setTxHash(null);
        record.setBlockNumber(null);
        recordRepository.save(record);
        BigInteger chainHeadBefore = chainHead();

        publishDuplicateEvent(anchored.id());
        RecordResponse reAnchored = awaitAnchored(anchored.id());

        assertThat(reAnchored.blockNumber()).isEqualTo(anchored.blockNumber());
        assertThat(reAnchored.txHash()).isEqualTo(anchored.txHash());
        assertThat(reAnchored.attempts()).isEqualTo(anchored.attempts() + 1);
        assertThat(chainHead()).isEqualTo(chainHeadBefore);
    }

    // --- helpers ---------------------------------------------------------

    private ResponseEntity<RecordResponse> submit(byte[] document) {
        return http.post()
                .uri("/api/records")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(fileUpload(document))
                .retrieve()
                .toEntity(RecordResponse.class);
    }

    private VerifyResponse verify(byte[] document) {
        return http.post()
                .uri("/api/verify")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(fileUpload(document))
                .retrieve()
                .body(VerifyResponse.class);
    }

    private RecordResponse getRecord(UUID id) {
        return http.get().uri("/api/records/{id}", id).retrieve().body(RecordResponse.class);
    }

    private RecordResponse submitAndAwaitAnchored(byte[] document) {
        return awaitAnchored(submit(document).getBody().id());
    }

    private RecordResponse awaitAnchored(UUID id) {
        return await().atMost(PIPELINE_TIMEOUT)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> getRecord(id), r -> r.status() == RecordStatus.ANCHORED);
    }

    /** Re-sends the record's original outbox payload, exactly as the relay would on a re-publish. */
    private void publishDuplicateEvent(UUID recordId) throws Exception {
        OutboxEvent original = outboxEventRepository.findAll().stream()
                .filter(e -> e.getRecordId().equals(recordId))
                .findFirst()
                .orElseThrow();
        kafkaTemplate
                .send(Topics.RECORDS_SUBMITTED, recordId.toString(), original.getPayload())
                .get(10, TimeUnit.SECONDS);
    }

    /**
     * Waits until the consumer group has committed an offset at the end of
     * records.submitted — i.e. it has finished processing everything
     * published so far, including the duplicate. Without this, "the record
     * didn't change" could just mean "the consumer hasn't got to it yet".
     */
    private void awaitConsumerCaughtUp() {
        TopicPartition partition = new TopicPartition(Topics.RECORDS_SUBMITTED, 0);
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, InfrastructureContainers.kafkaBootstrapServers()))) {
            await().atMost(PIPELINE_TIMEOUT).until(() -> {
                long end = admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                        .partitionResult(partition)
                        .get()
                        .offset();
                OffsetAndMetadata committed = admin.listConsumerGroupOffsets(consumerGroup)
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .get(partition);
                return committed != null && committed.offset() >= end;
            });
        }
    }

    private BigInteger chainHead() throws Exception {
        return web3j.ethBlockNumber().send().getBlockNumber();
    }

    private static byte[] uniqueDocument() {
        return ("test document " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    }

    private static MultiValueMap<String, Object> fileUpload(byte[] document) {
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        // A resource without a filename is sent as a plain form field, not
        // a file part; overriding getFilename() makes it a file upload.
        parts.add("file", new ByteArrayResource(document) {
            @Override
            public String getFilename() {
                return "document.txt";
            }
        });
        return parts;
    }
}
