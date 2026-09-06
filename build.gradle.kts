/*
 * Root build: only hosts the podman-compose lifecycle tasks used by the integration tests.
 * Every Java module applies the `fixflow.java-conventions` plugin from buildSrc.
 */
plugins {
    base
}

tasks.register<Exec>("kafkaUp") {
    group = "kafka"
    description = "Starts the single-node Kafka broker with `podman compose` and waits until it is ready."
    workingDir = projectDir
    commandLine("scripts/kafka-up.sh")
}

tasks.register<Exec>("kafkaDown") {
    group = "kafka"
    description = "Stops and removes the Kafka broker started with `podman compose`."
    workingDir = projectDir
    commandLine("scripts/kafka-down.sh")
}
