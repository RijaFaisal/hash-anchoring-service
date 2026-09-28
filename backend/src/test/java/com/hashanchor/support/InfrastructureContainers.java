package com.hashanchor.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Throwaway Postgres and Kafka for tests, started once per test JVM and
 * shared by every test class that asks for them. Same images as
 * docker-compose.yml, but on random host ports, so they never collide with
 * (or depend on) a compose stack that happens to be running.
 *
 * <p>Why a static singleton rather than {@code @Testcontainers} +
 * {@code @Container} on each test class: those stop the containers when
 * the class finishes, but Spring caches application contexts across test
 * classes — a cached context would then point at a dead database.
 * Starting them once and letting Testcontainers' reaper container remove
 * them when the JVM exits avoids that.
 */
public final class InfrastructureContainers {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        // Start both in parallel; blocks until each is accepting connections.
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    private InfrastructureContainers() {}

    /**
     * Points Spring at the containers. Call from a test class's
     * {@code @DynamicPropertySource} method; these values override
     * application.yml's localhost defaults.
     */
    public static void register(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    public static String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
