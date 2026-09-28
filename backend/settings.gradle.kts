plugins {
    // Lets Gradle auto-download a JDK 21 toolchain (from the Foojay Disco
    // API) when one isn't already installed locally, instead of failing.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "backend"
