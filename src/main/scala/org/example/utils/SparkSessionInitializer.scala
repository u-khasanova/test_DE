package org.example.utils

import org.apache.spark.sql.SparkSession

object SparkSessionInitializer {
  def init(): SparkSession = {
    val javaOptions = Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dlog4j2.configurationFile=log4j2.xml",
      "-Dlog4j2.disable.jmx=true",
      "--illegal-access=deny"
    ).mkString(" ")

    SparkSession
      .builder()
      .appName("SessionParser")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.extraJavaOptions", javaOptions)
      .getOrCreate()
  }
}
