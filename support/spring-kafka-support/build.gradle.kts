plugins {
    id("fixflow.java-conventions")
    `java-library`
}

description = "Spring Kafka helpers shared by the Spring Integration demos (manual-ack listener containers)."

dependencies {
    api(platform(libs.spring.boot.bom))
    api(libs.spring.kafka)
    implementation(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}
