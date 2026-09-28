package com.hashanchor;

import com.hashanchor.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * Boots the whole application — Flyway migrations, JPA validation, Kafka
 * listeners, scheduled jobs, web server — against the test containers.
 * Catches wiring and configuration mistakes that unit tests can't. The
 * pipeline's behaviour is covered by {@code AnchoringPipelineIntegrationTest}.
 */
class HashAnchorBackendApplicationTests extends AbstractIntegrationTest {

	@Test
	void contextLoads() {
	}

}
