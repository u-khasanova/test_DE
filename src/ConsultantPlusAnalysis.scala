package org.example
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ConsultantPlusAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now())

      // 1.0 upload data
      val inputRDD = sc.textFile("src/main/resources/data/???").cache()
      println(s"Total lines loaded: ${inputRDD.count()}")

      // the code below doesn't work properly
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
      val (parsedDOEvents, parseErrors) = DOEventParser.parseDORecords(doEventsRDD)
      val cachedValid = parsedDOEvents.cache()
      val cachedErrors = parseErrors.cache()

      // 8. Count ACC_45616 searches through CS
      val acc45616SearchCount = parsedCSEvents
        .filter(_.codes.contains("ACC_45616"))
        .count()
      println(s"Number of searches for document ACC_45616 by card: ${acc45616SearchCount}")

      // 9. Logging statistics for DOC_OPEN events
      println(s"Total DOC_OPEN events: ${doEventsRDD.count()}")
      println(s"Successfully parsed: ${cachedValid.count()}")
      println(s"Failed to parse: ${cachedErrors.count()}")

      // Save errors to a file
      cachedErrors.map(e => s"${e.error}\t${e.rawLine}")
        .coalesce(1)
        .saveAsTextFile(s"logs/doc_open_parse_errors_$timestamp")

      // Convert the date to yyyy-MM-dd format
      def reformatDate(originalDate: String): String = {
        val formatterInput = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy")
        val formatterOutput = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
        java.time.LocalDate.parse(originalDate, formatterInput).format(formatterOutput)
      }

      // Collecting QS ID as a broadcast variable
      val qsIds = parsedQSEvents
        .map(_.id)
        .distinct()
        .toLocalIterator
        .toSet
      val broadcastQsIds = sc.broadcast(qsIds)

      // 10. Calculating the total amount of DOC_OPEN through QS
      val dailyStatsRDD = cachedValid
        .filter(doc => broadcastQsIds.value.contains(doc.id))
        .mapPartitions { docs =>
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
          docs.map { doc =>
            val dateFormatted = doc.date.date.format(formatter)
            ((dateFormatted, doc.code), 1)
          }
        }
        .reduceByKey(_ + _, numPartitions = 200)
        .cache()

      val totalDocOpenWithQsId = dailyStatsRDD
        .map(_._2)
        .aggregate(0)(_ + _, _ + _)
      println(s"Total number of DOC_OPEN through QS: $totalDocOpenWithQsId")

      // Continue processing
      dailyStatsRDD
        .map { case ((date, doc), count) =>
          new java.lang.StringBuilder()
            .append(s"${reformatDate(date)}").append(",DOC_OPEN,")
            .append(doc).append(",")
            .append(count)
            .toString()
        }
        .sortBy(identity, false, 200)
        .repartition(1)
        .saveAsTextFile(s"output/daily_doc_stats_rdd_$timestamp")

      // Aggregate by date
      val docsByDateRDD = dailyStatsRDD
        .map { case ((date, doc), count) =>
          (doc, (date, count))
        }
        .groupByKey()
        .mapValues { datesWithCounts =>
          val dates = datesWithCounts.map(_._1).toList.sorted
          val totalCount = datesWithCounts.map(_._2).sum
          (totalCount, dates)
        }
        .sortBy(_._2._1, false)

      // Save to separate file
      docsByDateRDD
        .map { case (doc, (count, dates)) =>
          val datesString = dates.mkString(",")
          s"DOC_OPEN,$doc,$count,$datesString"
        }
        .repartition(1)
        .saveAsTextFile(s"output/docs_aggregated_by_date_$timestamp")

      // Errors analysis - example ???
      val emptyDateErrors = cachedErrors.filter(_.error == "Empty date field").count()
      println(s"\nDocuments with empty date: $emptyDateErrors")
    } finally {
      spark.stop()
    }
  }
}