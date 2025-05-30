package org.example.utils

import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}
import java.io.{File, FileInputStream}

object LoggerInitializer {
  def init(): Unit = {
    val configFile = new File("src/main/resources/log4j2.xml")
    System.setProperty("log4j.configurationFile", configFile.getAbsolutePath)
    System.setProperty("log4j2.disable.jmx", "true")
    val source = new ConfigurationSource(new FileInputStream(configFile), configFile)
    Configurator.initialize(null, source)
  }
}
