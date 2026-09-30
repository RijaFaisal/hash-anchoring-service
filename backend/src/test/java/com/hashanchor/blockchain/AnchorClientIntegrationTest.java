package com.hashanchor.blockchain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hashanchor.support.HardhatChain;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
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
 * <p>The chain is {@link HardhatChain}: a Hardhat node container with
 * HashAnchor freshly deployed, so the only prerequisite is Docker.
 * Tagged {@code integration}: run with {@code ./gradlew integrationTest}.
 */
@Tag("integration")
class AnchorClientIntegrationTest {

    private static Web3j web3j;
    private AnchorClient anchorClient;

    @BeforeAll
    static void connect() {
        web3j = Web3j.build(new HttpService(HardhatChain.rpcUrl()));
    }

    @AfterAll
    static void disconnect() {
        web3j.shutdown();
    }

    @BeforeEach
    void setUpClient() {
        anchorClient = clientRequiring(0, Duration.ofSeconds(60));
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

    @Test
    void awaitConfirmationsWithZeroConfirmationsReturnsTheGivenBlockAtOnce() throws Exception {
        assertThat(anchorClient.awaitConfirmations(randomDocHash(), 123))
                .isEqualTo(new VerifyResult(true, 123));
    }

    @Test
    void awaitConfirmationsReturnsOnceEnoughBlocksAreMinedOnTop() throws Exception {
        byte[] docHash = randomDocHash();
        AnchorResult anchored = anchorClient.anchor(docHash);
        // The Hardhat node mines one block per transaction, so two more
        // anchors put two blocks on top of the first.
        anchorClient.anchor(randomDocHash());
        anchorClient.anchor(randomDocHash());

        VerifyResult confirmed = clientRequiring(2, Duration.ofSeconds(5))
                .awaitConfirmations(docHash, anchored.blockNumber());

        assertThat(confirmed).isEqualTo(new VerifyResult(true, anchored.blockNumber()));
    }

    @Test
    void awaitConfirmationsTimesOutWhenNoBlocksArrive() throws Exception {
        byte[] docHash = randomDocHash();
        AnchorResult anchored = anchorClient.anchor(docHash);

        // No further transactions, so the node mines no further blocks.
        assertThatThrownBy(() -> clientRequiring(3, Duration.ofSeconds(1))
                        .awaitConfirmations(docHash, anchored.blockNumber()))
                .isInstanceOf(TimeoutException.class);
    }

    private static AnchorClient clientRequiring(int confirmations, Duration confirmationTimeout) {
        var properties = new BlockchainProperties(
                HardhatChain.contractAddress(),
                31337,
                100_000,
                null,
                Duration.ofMillis(200),
                Duration.ofSeconds(30),
                confirmations,
                confirmationTimeout);
        return new AnchorClient(web3j, Credentials.create(HardhatChain.DEV_PRIVATE_KEY), properties);
    }

    private static byte[] randomDocHash() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}
