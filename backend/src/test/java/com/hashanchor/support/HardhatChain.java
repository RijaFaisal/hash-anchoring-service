package com.hashanchor.support;

import com.hashanchor.blockchain.generated.HashAnchor;
import java.time.Duration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.tx.gas.DefaultGasProvider;

/**
 * A Hardhat dev chain in a container, with {@code HashAnchor} deployed to
 * it — started once per test JVM, like {@link InfrastructureContainers}.
 *
 * <p>The image comes from {@code src/test/resources/hardhat/Dockerfile}
 * (plain Hardhat, same version as contracts/). It's kept after the run
 * under a fixed tag so later runs skip the ~1 minute {@code npm install}.
 * The contract is deployed from Java with the web3j wrapper's
 * {@code deploy()}, which carries the compiled bytecode from the same
 * Hardhat artifact the app is built against — so no Node toolchain is
 * needed on the host to run the tests.
 */
public final class HardhatChain {

    /**
     * Account #0 of Hardhat's built-in dev chain. Derived from the public
     * test mnemonic every Hardhat/Anvil install uses — it's printed by
     * {@code npx hardhat node} and holds only fake ETH on a throwaway
     * chain, so it isn't a secret. Deploying with it makes it the
     * contract's owner, which is what lets the app call {@code anchor()}.
     */
    public static final String DEV_PRIVATE_KEY =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    private static final int RPC_PORT = 8545;

    @SuppressWarnings("resource") // lives for the whole JVM; the reaper removes it
    private static final GenericContainer<?> NODE = new GenericContainer<>(
                    new ImageFromDockerfile("hash-anchor-test-hardhat:3.18.0", false)
                            .withFileFromClasspath("Dockerfile", "hardhat/Dockerfile"))
            .withExposedPorts(RPC_PORT)
            .waitingFor(Wait.forLogMessage(".*Started HTTP and WebSocket JSON-RPC server.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    private static final String CONTRACT_ADDRESS;

    static {
        NODE.start();
        Web3j web3j = Web3j.build(new HttpService(rpcUrl()));
        try {
            CONTRACT_ADDRESS = HashAnchor.deploy(web3j, Credentials.create(DEV_PRIVATE_KEY), new DefaultGasProvider())
                    .send()
                    .getContractAddress();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deploy HashAnchor to the test chain", e);
        } finally {
            web3j.shutdown();
        }
    }

    private HardhatChain() {}

    public static String rpcUrl() {
        return "http://" + NODE.getHost() + ":" + NODE.getMappedPort(RPC_PORT);
    }

    public static String contractAddress() {
        return CONTRACT_ADDRESS;
    }

    /** Points the app's blockchain settings at this chain and contract. */
    public static void register(DynamicPropertyRegistry registry) {
        registry.add("hashanchor.blockchain.rpc-url", HardhatChain::rpcUrl);
        registry.add("hashanchor.blockchain.private-key", () -> DEV_PRIVATE_KEY);
        registry.add("hashanchor.blockchain.contract-address", HardhatChain::contractAddress);
    }
}
