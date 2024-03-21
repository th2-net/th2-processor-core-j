@file:Suppress("SpellCheckingInspection")

plugins {
    kotlin("jvm") version "1.8.22"
    kotlin("kapt") version "1.8.22"
    `java-library`
    `maven-publish`
    id("com.exactpro.th2.gradle.base") version "0.0.4"
    id("com.exactpro.th2.gradle.publish") version "0.0.4"
}

group = "com.exactpro.th2"
version = project.findProperty("release_version") as String

java {
    withJavadocJar()
    withSourcesJar()
}

kotlin {
    jvmToolchain(11)
}

repositories {
    mavenLocal()
    mavenCentral()
    maven {
        name = "Sonatype_snapshots"
        url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
    }
    maven {
        name = "Sonatype_releases"
        url = uri("https://s01.oss.sonatype.org/content/repositories/releases/")
    }

    configurations.all {
        resolutionStrategy.cacheChangingModulesFor(0, "seconds")
        resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
    }
}

dependencies {
    api("com.exactpro.th2:grpc-common:4.4.0-dev")
    implementation("com.exactpro.th2:common:5.10.0-dev")
    implementation("com.exactpro.th2:common-utils:2.2.2-dev")
    implementation("com.exactpro.th2:grpc-lw-data-provider:2.3.0-dev")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation("io.github.microutils:kotlin-logging:3.0.5")

    testCompileOnly("com.google.auto.service:auto-service:1.1.1")
    kaptTest("com.google.auto.service:auto-service:1.1.1")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.1.0")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

tasks {
    test {
        useJUnitPlatform {
            excludeTags("integration-test")
        }
    }

    register<Test>("integrationTest") {
        group = "verification"
        useJUnitPlatform {
            includeTags("integration-test")
        }
    }
}
