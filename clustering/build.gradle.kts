import java.util.Properties

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)

    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    `maven-publish`
    idea
}
val myVersion = System.getenv("MY_VERSION")?.let { v: String ->
    if (v.isNotBlank()) v
    else null
} ?: rootDir.resolve("version.txt").let { f ->
    if (f.exists() && f.isFile) f.readText().trim() else "1.0.0"
}

group = "me.hchome"
version = myVersion

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

val secretPropsFile = project.rootProject.file(".aws/aws-credentials.properties")
if (secretPropsFile.exists()) {
    secretPropsFile.reader().use {
        Properties().apply { load(it) }
    }.onEach { (name, value) ->
        ext[name.toString()] = value
    }
} else {
    ext["aws.accessKeyId"] = System.getenv("AWS_ACCESS_KEY_ID")
    ext["aws.secretAccessKey"] = System.getenv("AWS_SECRET_ACCESS_KEY")
    ext["aws.s3BucketUrl"] = System.getenv("AWS_S3_BUCKET_URL")
}


dependencies {
    implementation(project(":lib"))
    compileOnly(kotlin("stdlib"))
    compileOnly(kotlin("reflect"))
    compileOnly(libs.coroutines)
    implementation(libs.jetcd)
    testImplementation(kotlin("test"))
    testImplementation(kotlin("stdlib"))
    testImplementation(kotlin("reflect"))
    testImplementation(libs.coroutines)
    testImplementation(libs.coroutines.test)
}


tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
    compilerOptions {
        freeCompilerArgs.addAll( "-Xjvm-default=all", "-Xcontext-parameters" )
        languageVersion = org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_2
    }
}

java {
    withJavadocJar()
    withSourcesJar()
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

idea {

    module {
        isDownloadJavadoc = true
        isDownloadSources = true
    }
}

tasks.named("publish") {
    dependsOn("javadocJar", "sourcesJar")
}
fun getExtraString(name: String) = ext[name]?.toString()

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "me.hchome"
            artifactId = "kactor-clustering"
            version = project.version.toString()

            from(components["kotlin"])
            artifact(tasks.named("sourcesJar").get())
            artifact(tasks.named("javadocJar").get())
        }

    }

    repositories {
        maven {
            name = "S3"
            url = uri(getExtraString("aws.s3BucketUrl")?.let { "$it/releases" }
                ?: error("S3 bucket URL is not configured."))
            credentials(AwsCredentials::class) {
                accessKey = getExtraString("aws.accessKeyId") ?: error("AWS access key ID is not configured.")
                secretKey =
                    getExtraString("aws.secretAccessKey") ?: error("AWS secret access key is not configured.")
            }
        }
    }
}
