package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import scala.util.matching.Regex

object QSEventParser {
  case class ParsedQSRecord(id: String, date: DateTimeParts, codes: List[String])

  // Изменения в паттерне (делаем codes опциональным):
  private val qsPattern: Regex =
    """^QS\s+([^\s]+)\s+\{[^}]+\}\s+([\w-]*\d+)\s+((?:\w+_\d+\s*)*)""".r  // * вместо + в конце

  def parseQSLine(line: String): Option[ParsedQSRecord] = {
    line match {
      case qsPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr).map { dtParts =>
          ParsedQSRecord(
            id,
            dtParts,
            // Обрабатываем пустые коды:
            if (codesStr.trim.isEmpty) Nil else codesStr.trim.split("\\s+").toList
          )
        }
      case _ => None
    }
  }

  // parseQSRecords остается без изменений
  def parseQSRecords(rdd: RDD[String]): RDD[ParsedQSRecord] = {
    rdd.mapPartitions(_.flatMap(parseQSLine))
  }
}