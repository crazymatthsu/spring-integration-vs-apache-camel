pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "spring-integration-vs-apache-camel"

include(
    "common",
    "support:spring-kafka-support",
    "support:camel-kafka-support",
    "usecase1:spring-integration",
    "usecase1:apache-camel",
    "usecase2:spring-integration",
    "usecase2:apache-camel",
)
