package org.example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.example.DOEventParser.{parseDOLine, parseDORecords}

import scala.util.matching.Regex

object DOEventParser {
  case class ParsedDORecord(id: String, date: DateTimeParts, code: String)
  case class ParseError(rawLine: String, error: String)
  private val doPattern: Regex =
    """^DOC_OPEN\s+([^\s]*)\s+([\w-]*\d+)\s+(\w+_\d+)\s*$""".r

  def parseDOLine(line: String): Either[ParseError, ParsedDORecord] = {
    line match {
      case doPattern(dateStr, id, codeStr) =>
        if (dateStr.isEmpty) {
          Left(ParseError(line.take(100), "Empty date field"))
        } else {
          DateTimeParser.parse(dateStr) match {
            case Some(dtParts) =>
              val cleanedId = if (id.startsWith("-")) id.substring(1) else id
              Right(ParsedDORecord(cleanedId, dtParts, codeStr))
            case None =>
              Left(ParseError(line.take(100), s"Invalid date format: $dateStr"))
          }
        }
      case _ => Left(ParseError(line.take(100), "Pattern mismatch"))
    }
  }

  def parseDORecords(rdd: RDD[String]): (RDD[ParsedDORecord], RDD[ParseError]) = {
    val parsedResults = rdd.map(parseDOLine).cache()

    val successRecords = parsedResults.flatMap {
      case Right(record) => Some(record)
      case _ => None
    }

    val errorLogs = parsedResults.flatMap {
      case Left(error) => Some(error)
      case _ => None
    }

    (successRecords, errorLogs)
  }
}

object DOEventParserTest extends App {
  val testData = Seq(
    "DOC_OPEN 19.08.2020_01:51:34 13693734 LAW_140194",
    "DOC_OPEN  13693734 LAW_140194",
    "DOC_OPEN invalid_date 13693734 LAW_140194",
    "DOC_OPEN 19.08.2020_01:51:34 -13693734 LAW_140194",
    "INVALID FORMAT LINE",
    "DOC_OPEN 19.08.2020_01:51:34 13693734",
    "DOC_OPEN 2020-08-19T01:51:34 13693734 LAW_140194"
  )

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
  try {
    println("="*50)
    println("Starting DOEventParser tests")
    println("="*50 + "\n")

    println("Тестирование parseDOLine:")
    testData.foreach { line =>
      println(s"\nСтрока: ${line.take(60)}")
      parseDOLine(line) match {
        case Right(record) =>
          println("Успешный парсинг:")
          println(s"ID: ${record.id}")
          println(s"Дата: ${record.date.date}")
          println(s"Код: ${record.code}")
        case Left(error) =>
          println(s"Ошибка: ${error.error}")
          println(s"Контекст: ${error.rawLine}")
      }
    }

    println("\n" + "="*50)
    println("Тестирование parseDORecords (RDD):")
    val rdd = sc.parallelize(testData)
    val (successRecords, errorLogs) = parseDORecords(rdd)

    println(s"\nУспешно распарсено: ${successRecords.count()}")
    successRecords.take(3).foreach { record =>
      println(s"ID: ${record.id}, Дата: ${record.date.date}, Код: ${record.code}")
    }

    println(s"\nОшибок парсинга: ${errorLogs.count()}")
    errorLogs.take(3).foreach { error =>
      println(s"Ошибка: ${error.error}")
      println(s"Контекст: ${error.rawLine}")
    }
  } finally {
    sc.stop()
  }
}