package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import scala.util.matching.Regex

object SessionEventParser {
  case class ParsedSessionRecord(dateStart: DateTimeParts, dateEnd: DateTimeParts)

  private val sessionPattern: Regex =
    """^SESSION_START\s+([^\s]+).*?SESSION_END\s+([^\s]+).*?""".r

  def parseSessionLine(line: String): Option[ParsedSessionRecord] = {
    line match {
      case sessionPattern(startDateStr, endDateStr) =>  // Исправлено: разные имена для групп
        for {
          start <- DateTimeParser.parse(startDateStr)
          end <- DateTimeParser.parse(endDateStr)
        } yield ParsedSessionRecord(start, end)
      case _ => None
    }
  }

  def parseSessionRecords(rdd: RDD[String]): RDD[ParsedSessionRecord] = {
    rdd.mapPartitions(_.flatMap(parseSessionLine))
  }
}