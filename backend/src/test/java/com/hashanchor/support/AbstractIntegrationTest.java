package com.hashanchor.support;

import org.junit.jupiter.api.Tag;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for tests that boot the full application against real
 * Postgres, Kafka and Hardhat containers.
 *
 * <p>Every such test must extend this rather than configure its own
 * context. Spring caches application contexts by their configuration, so
 * identical configuration means one shared context — and there must be only
 * one: all contexts in the JVM talk to the same Kafka container, and two
 * live contexts would mean two anchoring consumers in the same group
 * competing for the same partition, one of them possibly misconfigured.
 *
 * <p>Annotations on a superclass are inherited by subclasses, so each test
 * class gets {@code @Tag("integration")} (runs under
 * {@code ./gradlew integrationTest}, not {@code test}) and the same
 * {@code @SpringBootTest} setup.
 */
@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        InfrastructureContainers.register(registry);
        HardhatChain.register(registry);
        // The 5s production default would make every test wait up to 5s
        // for the relay to notice its submission.
        registry.add("hashanchor.outbox.poll-interval", () -> "200ms");
    }
}
