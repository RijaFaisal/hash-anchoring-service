package com.hashanchor.blockchain;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

/**
 * Exercises {@link AnchorClient} against a real chain — deliberately not a
 * {@code @SpringBootTest}: it builds {@link AnchorClient} directly with
 * {@code new}, bypassing Spring entirely, so this test doesn't need the
 * whole application context (and therefore no Postgres/Kafka) just to
 * check that anchor()/verify() talk to a contract correctly.
 *
 * <p>Requires, before running:
 *
 * <ol>
 *   <li>{@code cd contracts && npx hardhat node} (leave running)
 *   <li>{@code npx hardhat ignition deploy ignition/modules/HashAnchor.ts --network localhost}
 *   <li>Export {@code HASHANCHOR_PRIVATE_KEY} (the deployer account printed
 *       by the node — it's also the contract owner) and
 *       {@code HASHANCHOR_CONTRACT_ADDRESS} (printed by the deploy command)
 * </ol>
 *
 * <p>Tagged {@code integration} so the default {@code ./gradlew test} skips
 * it; run it explicitly with {@code ./gradlew integrationTest}.
 */
@Tag("integration")
class AnchorClientIntegrationTest {

    private static Web3j web3j;
    private AnchorClient anchorClient;

    @BeforeAll
    static void connect() {
        String rpcUrl = System.getenv().getOrDefault("HASHANCHOR_RPC_URL", "http://localhost:8545");
        web3j = Web3j.build(new HttpService(rpcUrl));
    }

    @AfterAll
    static void disconnect() {
        web3j.shutdown();
    }

    @BeforeEach
    void setUpClient() {
        Credentials credentials = Credentials.create(requireEnv("HASHANCHOR_PRIVATE_KEY"));
        anchorClient = new AnchorClient(
                web3j,
                credentials,
                requireEnv("HASHANCHOR_CONTRACT_ADDRESS"),
                Duration.ofMillis(500),
                Duration.ofSeconds(30));
    }

    @Test
    void anchorReturnsTheTransactionHashAndBlockNumberItLandedIn() throws Exception {
        byte[] docHash = randomDocHash();

        AnchorResult result = anchorClient.anchor(docHash);

        assertThat(result.transactionHash()).matches("0x[0-9a-fA-F]{64}");
        assertThat(result.blockNumber()).isPositive();
    }

    @Test
    void verifyReflectsWhatAnchorJustWrote() throws Exception {
        byte[] docHash = randomDocHash();

        VerifyResult beforeAnchoring = anchorClient.verify(docHash);
        assertThat(beforeAnchoring.anchored()).isFalse();

        AnchorResult anchored = anchorClient.anchor(docHash);

        VerifyResult afterAnchoring = anchorClient.verify(docHash);
        assertThat(afterAnchoring.anchored()).isTrue();
        assertThat(afterAnchoring.blockNumber()).isEqualTo(anchored.blockNumber());
    }

    @Test
    void findAnchorTransactionHashRecoversTheTransactionFromTheEventLog() throws Exception {
        byte[] docHash = randomDocHash();
        AnchorResult anchored = anchorClient.anchor(docHash);

        assertThat(anchorClient.findAnchorTransactionHash(docHash, anchored.blockNumber()))
                .contains(anchored.transactionHash());
        // Right block, wrong hash: nothing.
        assertThat(anchorClient.findAnchorTransactionHash(randomDocHash(), anchored.blockNumber()))
                .isEmpty();
    }

    @Test
    void getBlockTimestampReturnsWhenTheAnchoringBlockWasMined() throws Exception {
        Instant before = Instant.now().minusSeconds(60);
        AnchorResult anchored = anchorClient.anchor(randomDocHash());

        assertThat(anchorClient.getBlockTimestamp(anchored.blockNumber()))
                .isBetween(before, Instant.now().plusSeconds(60));
    }

    private static byte[] randomDocHash() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be set to run this integration test");
        }
        return value;
    }
}
