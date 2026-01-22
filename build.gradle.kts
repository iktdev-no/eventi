import java.io.ByteArrayOutputStream

plugins {
    kotlin("jvm") version "2.2.10"
    id("maven-publish")
}

group = "no.iktdev"
version = "1.0-SNAPSHOT"
val named = "eventi"

repositories {
    mavenCentral()
}


dependencies {

    implementation("com.google.code.gson:gson:2.8.9")


    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")

    //testImplementation(kotlin("test"))
    testImplementation("org.assertj:assertj-core:3.4.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")

    testImplementation("io.mockk:mockk:1.13.5")
    testImplementation("com.h2database:h2:2.2.220")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}

val reposiliteUrl = if (version.toString().endsWith("SNAPSHOT")) {
    "https://reposilite.iktdev.no/snapshots"
} else {
    "https://reposilite.iktdev.no/releases"
}

publishing {
    publications {
        create<MavenPublication>("reposilite") {
            artifactId = named
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set(named)
                version = project.version.toString()
                url.set(reposiliteUrl)
            }
            from(components["kotlin"])
        }
    }
    repositories {
        mavenLocal()
        maven {
            name = named
            url = uri(reposiliteUrl)
            credentials {
                username = System.getenv("reposiliteUsername")
                password = System.getenv("reposilitePassword")
            }
        }
    }
}

fun findLatestTag(): String {
    val stdout = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "describe", "--tags", "--abbrev=0")
        standardOutput = stdout
        isIgnoreExitValue = true
    }
    return stdout.toString().trim().removePrefix("v")
}

fun isSnapshotBuild(): Boolean {
    // Use environment variable or branch name to detect snapshot
    val ref = System.getenv("GITHUB_REF") ?: ""
    return ref.endsWith("/master") || ref.endsWith("/main")
}

fun getCommitsSinceTag(tag: String): Int {
    val stdout = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "rev-list", "$tag..HEAD", "--count")
        standardOutput = stdout
        isIgnoreExitValue = true
    }
    return stdout.toString().trim().toIntOrNull() ?: 0
}

val latestTag = findLatestTag()
val versionString = if (isSnapshotBuild()) {
    val parts = latestTag.split(".")
    val patch = parts.lastOrNull()?.toIntOrNull()?.plus(1) ?: 1
    val base = if (parts.size >= 2) "${parts[0]}.${parts[1]}" else latestTag
    val buildNumber = getCommitsSinceTag("v$latestTag")
    "$base.$patch-SNAPSHOT-$buildNumber"
} else {
    latestTag
}

version = versionString