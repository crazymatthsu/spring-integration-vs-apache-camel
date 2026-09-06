plugins {
    id("fixflow.java-conventions")
    `java-library`
}

description = "Camel Kafka extension shared by the Camel demos: manual commits issued from any thread, applied on the consumer thread."

dependencies {
    api(platform(libs.spring.boot.bom))
    api(platform(libs.camel.bom))
    api(platform(libs.camel.spring.boot.bom))
    api(libs.camel.kafka)
    implementation(libs.spring.context)
    implementation(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}
