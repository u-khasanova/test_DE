package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import scala.util.matching.Regex

object CSEventParser {
  case class ParsedCSRecord(id: String, date: DateTimeParts, codes: List[String])

  private val csPattern: Regex =
    """^CARD_SEARCH_START\s+([^\s]+).*?CARD_SEARCH_END\s+(\d+)\s+([^\s].*?)\s*$""".r

  def parseCSLine(line: String): Option[ParsedCSRecord] = {
    line match {
      case csPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr) match {
          case Some(dtParts) =>
            Some(ParsedCSRecord(id, dtParts, codesStr.trim.split("\\s+").toList))
          case None =>
            System.err.println(s"Failed to parse date in: ${line.take(100)}")
            None
        }
      case _ =>
        System.err.println(s"Line doesn't match CS pattern: ${line.take(100)}")
        None
    }
  }

  def parseCSRecords(rdd: RDD[String]): RDD[ParsedCSRecord] = {
    rdd.flatMap(parseCSLine)
  }
}