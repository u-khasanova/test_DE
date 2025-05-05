package org.example
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import org.apache.spark.sql.SparkSession
import org.example.parser.SessionParser
import java.io.File
import java.io.FileInputStream

object MainApp {
  System.setProperty("log4j.configurationFile", "log4j2.xml")
  System.setProperty("log4j2.disable.jmx", "true")

  val configFile = new File("src/resources/log4j2.xml")
  val source = new ConfigurationSource(new FileInputStream(configFile), configFile)
  Configurator.initialize(null, source)

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  logger.warn("Тест логирования - это сообщение должно появиться")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .config("spark.ui.enabled", "false")  // Отключаем веб-интерфейс
      .config("spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
          "-Dlog4j2.configurationFile=log4j2.xml " +
          "-Dlog4j2.disable.jmx=true " +
          "--illegal-access=deny")
      .getOrCreate()

    val sc = spark.sparkContext
    val input = sc.textFile("src/resources/data/1")

    val parsedSession = SessionParser.parse(input)
    parsedSession.take(10).foreach { session =>
      println(s"""
                 |Session ${session.startDate.date} ${session.startDate.time} - ${session.endDate.time}
                 |QS: ${session.QS.size}
                 |CardSearch: ${session.cardSearch.size}
                 |DocOpen: ${session.docOpen.size}
                 |""".stripMargin)
    }





      spark.stop()
    }
}