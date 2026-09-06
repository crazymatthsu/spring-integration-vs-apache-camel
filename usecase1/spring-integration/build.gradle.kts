plugins {
    id("fixflow.java-conventions")
    alias(libs.plugins.spring.boot)
}

description = "usecase1 implemented with Spring Integration"

base {
    archivesName.set("usecase1-spring-integration")
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(project(":common"))
    implementation(project(":support:spring-kafka-support"))

    implementation(libs.spring.boot.starter.integration)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.spring.boot.starter.kafka)
    implementation(libs.spring.integration.kafka)
    implementation(libs.spring.integration.jdbc)

    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly(libs.junit.platform.launcher)

    "integrationTestImplementation"(testFixtures(project(":common")))
}
