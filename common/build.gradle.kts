plugins {
    id("fixflow.java-conventions")
    `java-library`
    `java-test-fixtures`
}

description = "Framework-neutral code shared by all demos: FIX 4.2 parsing, order model, SQLite persistence, Kafka test support."

dependencies {
    api(platform(libs.spring.boot.bom))

    api(libs.quickfixj.core)
    api(libs.quickfixj.messages.fix42)
    api(libs.spring.jdbc)
    api(libs.sqlite.jdbc)
    api(libs.kafka.clients)
    implementation(libs.slf4j.api)

    testFixturesApi(platform(libs.spring.boot.bom))
    testFixturesApi(libs.kafka.clients)
    testFixturesApi(libs.awaitility)
    testFixturesApi(libs.junit.jupiter)
    testFixturesApi(libs.assertj.core)
    testFixturesImplementation(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

tasks.register<JavaExec>("sendOrders") {
    group = "demo"
    description = "Publishes sample FIX 4.2 NewOrderSingle messages to Kafka. Options: -Ptopic=orders -Pcount=100 -PinvalidEvery=0"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.fixflow.common.tools.FixOrderProducer")
    args(
        (project.findProperty("topic") ?: "orders").toString(),
        (project.findProperty("count") ?: "100").toString(),
        (project.findProperty("invalidEvery") ?: "0").toString(),
    )
    systemProperty("fixflow.kafka.bootstrap", System.getenv("FIXFLOW_KAFKA_BOOTSTRAP") ?: "localhost:9092")
}
