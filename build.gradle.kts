@file:Suppress("SpellCheckingInspection")

import org.gradle.api.JavaVersion.VERSION_11
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.21"
    kotlin("kapt") version "1.6.21"
    `java-library`
    `maven-publish`
    signing
    id("org.owasp.dependencycheck") version "7.3.0"
    id("io.github.gradle-nexus.publish-plugin") version "1.0.0"
}

group = "com.exactpro.th2"
version = project.findProperty("release_version") ?: "1.0-SNAPSHOT"

java {
    sourceCompatibility = VERSION_11
    targetCompatibility = VERSION_11

    withJavadocJar()
    withSourcesJar()
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
    api(platform("com.exactpro.th2:bom:4.2.0"))

    implementation("com.exactpro.th2:common:5.1.1-dev")
    implementation("com.exactpro.th2:common-utils:2.0.0-dev")
    implementation("com.exactpro.th2:grpc-lw-data-provider:2.0.0-dev-version-2-4193884623-SNAPSHOT")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.14.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    testCompileOnly("com.google.auto.service:auto-service:1.0.1")
    kaptTest("com.google.auto.service:auto-service:1.0.1")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.mockito.kotlin:mockito-kotlin:4.0.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set(rootProject.name)
//                packaging.set("jar")
                description.set(rootProject.description)
                url.set(project.findProperty("vcs_url").toString())
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("developer")
                        name.set("developer")
                        email.set("developer@exactpro.com")
                    }
                }
                scm {
                    url.set(project.findProperty("vcs_url").toString())
                }
            }
        }
    }
    repositories {
//Nexus repo to publish from gitlab
        maven {
            name = "nexusRepository"
            credentials {
                username = project.findProperty("nexus_user").toString()
                password = project.findProperty("nexus_password").toString()
            }
            url = uri(project.findProperty("nexus_url").toString())
        }
    }
}

signing {
    useInMemoryPgpKeys(
        project.findProperty("signingKey").toString(),
        project.findProperty("signingPassword").toString()
    )
    sign(publishing.publications["maven"])
}

nexusPublishing {
    repositories {
        create("sonatype") {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
        }
    }
}

tasks {
    test {
        useJUnitPlatform {
            excludeTags("integration-test")
        }
    }

    jar {
        manifest {
            attributes(
                "Created-By" to "${System.getProperty("java.version")} (${System.getProperty("java.vendor")})",
                "Specification-Title" to "",
                "Specification-Vendor" to "Exactpro Systems LLC",
                "Implementation-Title" to project.displayName,
                "Implementation-Vendor" to "Exactpro Systems LLC",
                "Implementation-Vendor-Id" to "com.exactpro",
                "Implementation-Version" to project.version
            )
        }
    }

    register<Test>("integrationTest") {
        group = "verification"
        useJUnitPlatform {
            includeTags("integration-test")
        }
    }

    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "11"
    }

    // conditionals for publications
    withType<PublishToMavenRepository> {
        onlyIf {
            (repository == publishing.repositories["nexusRepository"] &&
                    project.hasProperty("nexus_user") &&
                    project.hasProperty("nexus_password") &&
                    project.hasProperty("nexus_url")) ||
                    (repository == publishing.repositories["sonatype"] &&
                            project.hasProperty("sonatypeUsername") &&
                            project.hasProperty("sonatypePassword"))
        }
    }
    withType<Sign> {
        onlyIf { project.hasProperty("signingKey") &&
                project.hasProperty("signingPassword")
        }
    }
}