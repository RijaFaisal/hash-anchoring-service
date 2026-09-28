package com.hashanchor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

// hashanchor.blockchain.private-key/contract-address have no defaults in
// application.yml by design (a missing real value should fail loudly), so
// this context-load smoke test needs *something* syntactically valid to let
// BlockchainConfig's beans construct. Nothing here calls anchor()/verify(),
// so these values are never actually used to talk to a chain — the address
// is the well-known Hardhat dev key's, not a real deployment.
//
// This context connects to the real docker compose Postgres/Kafka, so
// listener auto-startup is off: otherwise the anchoring consumer would start
// consuming records.submitted for real, against the dummy contract above.
@SpringBootTest
@TestPropertySource(
        properties = {
            "hashanchor.blockchain.private-key=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "hashanchor.blockchain.contract-address=0x5FbDB2315678afecb367f032d93F642f64180aa3",
            "spring.kafka.listener.auto-startup=false"
        })
class HashAnchorBackendApplicationTests {

	@Test
	void contextLoads() {
	}

}
