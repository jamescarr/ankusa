// packages/sdk-java: the Ankusa Java SDK, built with sbt 2 and published to
// Maven Central as io.github.jamescarr:ankusa-sdk. Every gate lives in the
// monorepo: `mise run check:package sdk-java` (javafmtCheckAll, testFull, doc,
// publish), and .github/workflows/release-maven.yml signs and uploads the
// bundle that `publish` stages in target/sona-staging via the Central Portal.
//
// sbt 2 caches `test`'s result on disk, so anything that must actually run
// (CI, conformance) uses `testFull` or `testOnly`.

organization := "io.github.jamescarr"
name := "ankusa-sdk"
version := "0.0.0"

description := "Client SDK for Ankusa, the self-hosted webhook receiver: claim-check redemption, delivery headers, and the routes, admin, and sources APIs."
homepage := Some(uri("https://github.com/jamescarr/ankusa/tree/main/packages/sdk-java"))
licenses := List(License.Apache2)
scmInfo := Some(ScmInfo(uri("https://github.com/jamescarr/ankusa"), "scm:git:https://github.com/jamescarr/ankusa.git"))
developers := List(Developer("jamescarr", "James Carr", "james.r.carr@gmail.com", uri("https://github.com/jamescarr")))
versionScheme := Some("early-semver")

// Java only: no Scala library in any classpath or the POM, no `_3` artifact
// suffix on the jar or the coordinates.
crossPaths := false
autoScalaLibrary := false

// Built on JDK 25, published as Java 17 bytecode against the Java 17 API.
// -serial off: these exceptions are never serialized. -proc:none: no annotation
// processors.
javacOptions ++= Seq("--release", "17", "-encoding", "UTF-8", "-proc:none", "-Xlint:all,-serial", "-Werror")
// := not ++=: javadoc rejects -Xlint/-proc, which it would otherwise inherit.
Compile / doc / javacOptions := Seq("--release", "17", "-encoding", "UTF-8", "-Xdoclint:all", "-Werror", "-quiet", "-notimestamp")

libraryDependencies ++= Seq(
  "tools.jackson.core" % "jackson-databind" % "3.2.3",
  "org.jspecify" % "jspecify" % "1.0.1",
  "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
)

Compile / packageBin / packageOptions += Package.ManifestAttributes("Automatic-Module-Name" -> "io.github.jamescarr.ankusa")

// Forked: the JDK HttpClient/HttpServer threads would outlive sbt's in-process
// test class loader (closeClassLoaders). The conformance runner reads the
// vectors the monorepo keeps outside the build.
Test / fork := true
Test / javaOptions += s"-Dankusa.conformance.cases=${(baseDirectory.value / ".." / ".." / "conformance" / "cases").getCanonicalPath}"

// Maven Central through the Central Portal: `publish` stages an unsigned bundle
// in target/sona-staging, `publishSigned` an armored-signature one, and
// `sonaRelease` uploads whichever is staged (release-maven.yml).
publishMavenStyle := true
pomIncludeRepository := { _ => false }
publishTo := localStaging.value

// No extra setting for the SBOM: sbt-sbom already registers its output in
// packagedArtifacts as Artifact(name, "cyclonedx", "json", Some("cyclonedx")),
// so `publish` stages it as target/.../ankusa-sdk-<v>-cyclonedx.json next to
// the jar, sources, javadoc and pom that release-maven.yml sign and upload.
