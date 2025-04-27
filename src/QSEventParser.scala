package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import scala.util.matching.Regex

object QSEventParser {
  case class ParsedQSRecord(id: String, date: DateTimeParts, codes: List[String])

  private val qsPattern: Regex =
    """^QS\s+([^\s]+)\s+\{[^}]+\}\s+(\d+)\s+((?:\w+_\d+\s*)+)""".r

  def parseQSLine(line: String): Option[ParsedQSRecord] = {
    line match {
      case qsPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr) match {
          case Some(dtParts) =>
            Some(ParsedQSRecord(id, dtParts, codesStr.trim.split("\\s+").toList))
          case None =>
            System.err.println(s"Failed to parse date in: ${line.take(100)}")
            None
        }
      case _ =>
        System.err.println(s"Line doesn't match QS pattern: ${line.take(100)}")
        None
    }
  }

  def parseQSRecords(rdd: RDD[String]): RDD[ParsedQSRecord] = {
    rdd.flatMap(parseQSLine)
  }
}