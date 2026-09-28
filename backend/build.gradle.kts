plugins {
	java
	id("org.springframework.boot") version "4.1.1"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "com.hashanchor"
version = "0.0.1-SNAPSHOT"
description = "hash-anchor Spring Boot backend"

val web3jVersion = "5.0.0"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

// A separate resolvable configuration for the code generator itself, kept
// off the app's own classpath — it's a build-time tool, not a runtime
// dependency of the backend.
val web3jCodegen by configurations.creating

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
	implementation("org.springframework.boot:spring-boot-starter-flyway")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.flywaydb:flyway-database-postgresql")
	implementation("org.web3j:core:$web3jVersion")
	web3jCodegen("org.web3j:codegen:$web3jVersion")
	compileOnly("org.projectlombok:lombok")
	runtimeOnly("org.postgresql:postgresql")
	annotationProcessor("org.projectlombok:lombok")
	testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
	testImplementation("org.springframework.boot:spring-boot-starter-flyway-test")
	testImplementation("org.springframework.boot:spring-boot-starter-validation-test")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.assertj:assertj-core")
	// Throwaway Postgres/Kafka (and Hardhat) containers per test run, so
	// tests never depend on docker compose being up. Versions come from
	// Spring Boot's dependency management (Testcontainers 2.x).
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testImplementation("org.testcontainers:testcontainers-kafka")
	// "Wait until this becomes true" polling for the async pipeline.
	testImplementation("org.awaitility:awaitility")
	testCompileOnly("org.projectlombok:lombok")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testAnnotationProcessor("org.projectlombok:lombok")
}

val web3jGeneratedDir = layout.buildDirectory.dir("generated/sources/web3j/main/java")
val hashAnchorArtifact = file("$rootDir/../contracts/artifacts/contracts/HashAnchor.sol/HashAnchor.json")
val web3jExtractedDir = layout.buildDirectory.dir("web3j/HashAnchor")

// The contract's ABI and bytecode live in contracts/artifacts, written by
// `npx hardhat build`. web3j's generator wants them as separate .abi/.bin
// files, so this task pulls both out of Hardhat's combined artifact JSON
// before codegen runs. Run `npx hardhat build` in contracts/ first if this
// task fails with a missing-file error.
val extractHashAnchorAbi by tasks.registering {
	group = "web3j"
	description = "Extracts the HashAnchor ABI and bytecode from the Hardhat build artifact"
	inputs.file(hashAnchorArtifact)
	outputs.dir(web3jExtractedDir)
	doLast {
		check(hashAnchorArtifact.exists()) {
			"Missing $hashAnchorArtifact — run `npx hardhat build` in contracts/ first"
		}
		val json = groovy.json.JsonSlurper().parse(hashAnchorArtifact) as Map<*, *>
		val abiJson = groovy.json.JsonOutput.toJson(json["abi"])
		val bytecode = (json["bytecode"] as String).removePrefix("0x")
		val dir = web3jExtractedDir.get().asFile.apply { mkdirs() }
		dir.resolve("HashAnchor.abi").writeText(abiJson)
		dir.resolve("HashAnchor.bin").writeText(bytecode)
	}
}

// Generates the typed Java wrapper (HashAnchor.java) that AnchorClient calls
// into — same idea as a JPA repository proxy, but for a smart contract's
// ABI instead of a database table.
val generateContractWrappers by tasks.registering(JavaExec::class) {
	group = "web3j"
	description = "Generates the web3j Java wrapper for the HashAnchor contract"
	dependsOn(extractHashAnchorAbi)
	inputs.dir(web3jExtractedDir)
	outputs.dir(web3jGeneratedDir)
	classpath = web3jCodegen
	mainClass.set("org.web3j.codegen.SolidityFunctionWrapperGenerator")
	doFirst { web3jGeneratedDir.get().asFile.mkdirs() }
	args(
		"-a", web3jExtractedDir.get().asFile.resolve("HashAnchor.abi").path,
		"-b", web3jExtractedDir.get().asFile.resolve("HashAnchor.bin").path,
		"-o", web3jGeneratedDir.get().asFile.path,
		"-p", "com.hashanchor.blockchain.generated",
	)
}

sourceSets {
	main {
		java {
			srcDir(web3jGeneratedDir)
		}
	}
}

tasks.named("compileJava") {
	dependsOn(generateContractWrappers)
}

tasks.withType<Test> {
	useJUnitPlatform()
}

// `test` runs only plain unit tests: no Docker, no network, fast.
// Anything tagged "integration" runs in the separate `integrationTest` task
// instead: those tests start their own Postgres, Kafka and Hardhat node
// containers via Testcontainers, so they need Docker running — but nothing
// else (no docker compose, no hand-started Hardhat node). The Hardhat image
// is built from src/test/resources/hardhat/Dockerfile; the first build takes
// a minute or so, later runs reuse the cached image.
tasks.named<Test>("test") {
	useJUnitPlatform {
		excludeTags("integration")
	}
}

val integrationTest by tasks.registering(Test::class) {
	group = "verification"
	description = "Runs tests tagged 'integration' (start Postgres, Kafka and Hardhat containers)"
	testClassesDirs = sourceSets["test"].output.classesDirs
	classpath = sourceSets["test"].runtimeClasspath
	useJUnitPlatform {
		includeTags("integration")
	}
	shouldRunAfter("test")
}
