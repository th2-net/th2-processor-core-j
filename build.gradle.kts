@file:Suppress("SpellCheckingInspection")

import org.gradle.api.JavaVersion.VERSION_11
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.21"
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
    api(platform("com.exactpro.th2:bom:4.0.2"))
    api("com.exactpro.th2:grpc-common:3.12.0")

    implementation("com.exactpro.th2:common:3.42.0")
    implementation("com.exactpro.th2:common-utils:0.0.1-dev-3190587875-SNAPSHOT")
    implementation("com.exactpro.th2:grpc-data-provider:1.1.0-TH2-4262-reduce-load-on-a-separate-boxes-in-crawler-schema-3218351603-SNAPSHOT")

    implementation("com.github.ajalt:clikt:2.8.0")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")

    testImplementation(kotlin("test"))
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