// Writes the Eclipse project files the jdtls language server reads.
//
// jdtls cannot import an sbt build, so `sbt jdtlsProject` materializes what it
// can read instead: .project (the Java builder and nature) and .classpath (the
// two source folders, the Java 17 JRE container, and one absolute lib path per
// test-scope dependency jar). JDT compiles into target/jdtls, never into sbt's
// target/out, so the two never fight over the same class files.
//
// Re-run it after changing libraryDependencies: the jar list is a snapshot
// taken when the task writes.

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import sbt._
import sbt.Keys._

object JdtlsPlugin extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = plugins.JvmPlugin

  object autoImport {
    val jdtlsProject = taskKey[Unit]("Write .project/.classpath for the jdtls language server")
  }

  import autoImport._

  /** Where JDT puts compiled classes; deliberately not sbt's target/out. */
  private val mainOutput = "target/jdtls/classes"

  private val testOutput = "target/jdtls/test-classes"

  private val jreContainer =
    "org.eclipse.jdt.launching.JRE_CONTAINER/" +
      "org.eclipse.jdt.internal.debug.ui.launcher.StandardVMType/JavaSE-17"

  override def projectSettings =
    Seq(
      // uncached: this task writes files, so sbt must never replay a cached
      // result and leave a stale .classpath behind.
      jdtlsProject := Def.uncached {
        val base = baseDirectory.value
        val converter = fileConverter.value
        val jars = (Test / externalDependencyClasspath).value
          .map(entry => converter.toPath(entry.data).toAbsolutePath.toString)
          .distinct

        write(base / ".project", projectFile)
        write(base / ".classpath", classpathFile(jars))

        val log = streams.value.log
        log.info(s"wrote .project and .classpath for ${base.getName} (${jars.size} jars)")
      }
    )

  private def projectFile: String =
    """<?xml version="1.0" encoding="UTF-8"?>
      |<projectDescription>
      |	<name>sdk-java</name>
      |	<comment></comment>
      |	<projects>
      |	</projects>
      |	<buildSpec>
      |		<buildCommand>
      |			<name>org.eclipse.jdt.core.javabuilder</name>
      |			<arguments>
      |			</arguments>
      |		</buildCommand>
      |	</buildSpec>
      |	<natures>
      |		<nature>org.eclipse.jdt.core.javanature</nature>
      |	</natures>
      |</projectDescription>
      |""".stripMargin

  private def classpathFile(jars: Seq[String]): String = {
    val entries =
      Seq(
        s"""	<classpathentry kind="src" output="$mainOutput" path="src/main/java"/>""",
        s"""	<classpathentry kind="src" output="$testOutput" path="src/test/java"/>""",
        s"""	<classpathentry kind="con" path="$jreContainer"/>"""
      ) ++ jars.map(jar => s"""	<classpathentry kind="lib" path="${escape(jar)}"/>""") ++
        Seq(s"""	<classpathentry kind="output" path="$mainOutput"/>""")

    ("""<?xml version="1.0" encoding="UTF-8"?>""" +: """<classpath>""" +: entries)
      .mkString("", "\n", "\n</classpath>\n")
  }

  /** The five XML entities a Windows-less absolute path can still contain. */
  private def escape(value: String): String =
    value
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&apos;")

  private def write(file: File, content: String): Unit =
    Files.writeString(file.toPath, content, StandardCharsets.UTF_8)
}
