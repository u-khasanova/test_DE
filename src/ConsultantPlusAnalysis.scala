package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.example.QSEventExtractor
import org.example.QSEventParser
import org.example.CSEventParser
import org.example.CSEventExtractor
import org.example.DOEventParser
import org.example.DOEventExtractor
import org.example.SessionEventExtractor
import org.example.SessionEventParser

object ConsultantPlusAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      val sc = spark.sparkContext

      // 1.0 upload data
      val inputRDD = sc.textFile("src/main/resources/data/*").cache()
      println(s"Total lines loaded: ${inputRDD.count()}")

//      // 1.1 extract SESSION_START & SESSION_END events
//      val sessionEventsRDD = SessionEventExtractor.extractEvents(inputRDD).cache()
//      println(s"Total successfully extracted SESSION events: ${sessionEventsRDD.count()}")
//
//      // 1.2 parse SESSION_START & SESSION_END events
//      val parsedSessionEvents = SessionEventParser.parseSessionRecords(sessionEventsRDD).cache()
//      parsedSessionEvents.foreach(println)

      // 2. extract QS events
      val qsEventsRDD = QSEventExtractor.extractEvents(inputRDD).cache()

      // 3. parse QS events
      val parsedQSEvents = QSEventParser.parseQSRecords(qsEventsRDD).cache()
      println(s"Total successfully parsed QS events: ${parsedQSEvents.count()}")

      // 4. extract CARD_SEARCH_START & CARD_SEARCH_END events
      val csEventsRDD = CSEventExtractor.extractEvents(inputRDD).cache()

      // 5. parse CARD_SEARCH_START & CARD_SEARCH_END events
      val parsedCSEvents = CSEventParser.parseCSRecords(csEventsRDD).cache()
      println(s"Total successfully parsed CS events: ${parsedCSEvents.count()}")

      // 6. extract DOC_OPEN events
      val doEventsRDD = DOEventExtractor.extractEvents(inputRDD).cache()

      // 7. parse DOC_OPEN events
      // Парсим события с разделением на валидные и ошибочные
      val (parsedDOEvents, parseErrors) = DOEventParser.parseDORecords(doEventsRDD)

      // 2. Подсчет ACC_45616 (распределенный)
      val acc45616SearchCount = parsedCSEvents
        .filter(_.codes.contains("ACC_45616"))
        .count()
      println(s"Количество поисков документа АСС_45616 по карточке: ${acc45616SearchCount}")

      // 1. Кэширование
      // Кэшируем оба RDD
      val cachedValid = parsedDOEvents.cache()
      val cachedErrors = parseErrors.cache()

      // Логируем статистику
      println(s"Total DOC_OPEN events: ${doEventsRDD.count()}")
      println(s"Successfully parsed: ${cachedValid.count()}")
      println(s"Failed to parse: ${cachedErrors.count()}")

      // Сохраняем ошибки в файл
      cachedErrors.map(e => s"${e.error}\t${e.rawLine}")
        .saveAsTextFile("logs/doc_open_parse_errors/")

      // Преобразуем дату в формат yyyy-MM-dd для корректной сортировки
      def reformatDate(originalDate: String): String = {
        val formatterInput = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy")
        val formatterOutput = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
        java.time.LocalDate.parse(originalDate, formatterInput).format(formatterOutput)
      }

      // Получаем статистику и преобразуем даты
      val dailyStats = cachedValid
        .map(doc => {
          val dateFormatted = reformatDate(doc.date.date) // Преобразуем dd.MM.yyyy → yyyy-MM-dd
          (dateFormatted, doc.code)
        })
        .countByValue() // (date, doc) -> count

      // Вариант для больших данных (чисто RDD)
      val dailyStatsRDD = cachedValid
        .map(doc => {
          val dateFormatted = reformatDate(doc.date.date)
          ((dateFormatted, doc.code), 1)
        })
        .reduceByKey(_ + _)
        .map { case ((date, doc), count) =>
          s"$date,DOC_OPEN,$doc,$count"
        }
        .sortBy(identity) // Сортировка по строке (уже в формате yyyy-MM-dd)
        .coalesce(1)

      dailyStatsRDD.saveAsTextFile("output/daily_doc_stats_rdd")

      // Дополнительный анализ ошибок (пример)
      val emptyDateErrors = cachedErrors.filter(_.error == "Empty date field").count()
      println(s"\nDocuments with empty date: $emptyDateErrors")


    } finally {
      spark.stop()
    }
  }
}