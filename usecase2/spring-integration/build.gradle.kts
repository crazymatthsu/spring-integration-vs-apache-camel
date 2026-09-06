plugins {
    id("fixflow.java-conventions")
    alias(libs.plugins.spring.boot)
}

description = "usecase2 implemented with Spring Integration"

base {
    archivesName.set("usecase2-spring-integration")
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(project(":common"))
    implementation(project(":support:spring-kafka-support"))

    implementation(libs.spring.boot.starter.integration)
    implementation(libs.spring.boot.starter.kafka)
    implementation(libs.spring.integration.kafka)

    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly(libs.junit.platform.launcher)

    "integrationTestImplementation"(testFixtures(project(":common")))
}
