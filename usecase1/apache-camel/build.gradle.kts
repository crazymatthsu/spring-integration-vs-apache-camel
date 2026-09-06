plugins {
    id("fixflow.java-conventions")
    alias(libs.plugins.spring.boot)
}

description = "usecase1 implemented with Apache Camel"

base {
    archivesName.set("usecase1-apache-camel")
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(platform(libs.camel.bom))
    implementation(platform(libs.camel.spring.boot.bom))
    implementation(project(":common"))
    implementation(project(":support:camel-kafka-support"))

    implementation(libs.spring.boot.starter)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.camel.spring.boot.starter)
    implementation(libs.camel.kafka.starter)
    implementation(libs.camel.sql.starter)

    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly(libs.junit.platform.launcher)

    "integrationTestImplementation"(testFixtures(project(":common")))
}
