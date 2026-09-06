/*
 * Shared conventions for every Java module in this build:
 *  - Java 21 bytecode (compiled with whatever JDK 21+ runs Gradle)
 *  - JUnit Platform for unit tests
 *  - a dedicated `integrationTest` source set + task that needs the Kafka broker from podman compose
 */
plugins {
    java
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint:deprecation"))
}

val integrationTest: SourceSet by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = false
    }
    // Where the integration tests (and the apps under test) find Kafka. Override with FIXFLOW_KAFKA_BOOTSTRAP.
    systemProperty("fixflow.kafka.bootstrap", System.getenv("FIXFLOW_KAFKA_BOOTSTRAP") ?: "localhost:9092")
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against the Kafka broker started by `podman compose` (see scripts/kafka-up.sh)."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    // Always re-run: the broker state is external to Gradle's inputs.
    outputs.upToDateWhen { false }
    dependsOn(":kafkaUp")
}
