plugins {
    kotlin("jvm") version "2.2.10"
}

group = "no.iktdev"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val exposedVersion = "0.61.0"

dependencies {

    implementation ("mysql:mysql-connector-java:8.0.29")
    implementation("org.jetbrains.exposed:exposed-core:${exposedVersion}")
    implementation("org.jetbrains.exposed:exposed-dao:${exposedVersion}")
    implementation("org.jetbrains.exposed:exposed-jdbc:${exposedVersion}")
    implementation("org.jetbrains.exposed:exposed-java-time:${exposedVersion}")

    implementation("com.google.code.gson:gson:2.8.9")


    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    //testImplementation(kotlin("test"))
    testImplementation("org.assertj:assertj-core:3.4.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")


    testImplementation("com.h2database:h2:2.2.220")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}