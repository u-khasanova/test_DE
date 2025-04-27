package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.example.QSEventExtractor
import org.example.QSEventParser
import org.example.CSEventParser
import org.example.CSEventExtractor
import org.example.DOEventParser
import org.example.DOEventExtractor

object ConsultantPlusAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      val sc = spark.sparkContext

      // 1. upload data
      val inputRDD = sc.textFile("src/main/resources/data/5").cache()
      println(s"Total lines loaded: ${inputRDD.count()}")

      // 2. extract QS events
      val qsEventsRDD = QSEventExtractor.extractEvents(inputRDD).cache()
      println(s"\nExtracted QS events: ${qsEventsRDD.count()}")

      // 3. parse QS events
      val parsedQSEvents = QSEventParser.parseQSRecords(qsEventsRDD).cache()
      println(s"Total successfully parsed QS events: ${parsedQSEvents.count()}")

      // 4. extract CS events
      val csEventsRDD = CSEventExtractor.extractEvents(inputRDD).cache()
      println(s"\nExtracted CS events: ${csEventsRDD.count()}")

      // 5. parse CS events
      val parsedCSEvents = CSEventParser.parseCSRecords(csEventsRDD).cache()
      println(s"Total successfully parsed CS events: ${parsedCSEvents.count()}")

      // 6. extract DOC_OPEN events
      val doEventsRDD = DOEventExtractor.extractEvents(inputRDD).cache()
      println(s"\nExtracted DOC_OPEN events: ${csEventsRDD.count()}")

      // 7. parse DOC_OPEN events
      val parsedDOEvents = DOEventParser.parseDORecords(doEventsRDD).cache()
      println(s"Total successfully parsed DOC_OPEN events: ${parsedDOEvents.count()}")

      val acc45616SearchCount = parsedCSEvents
        .filter(_.codes.contains("ACC_45616"))
        .count()

      println(s"\nДокумент ACC_45616 искали через поиск по карточке $acc45616SearchCount раз(а)")


    } finally {
      spark.stop()
    }
  }
}