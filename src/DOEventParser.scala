package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import scala.util.matching.Regex

object DOEventParser {
  case class ParsedDORecord(id: String, date: DateTimeParts, code: String)

  private val doPattern: Regex =
    """^DOC_OPEN\s+([^\s]+)\s+(\d+)\s+(\w+_\d+)\s*$""".r

  def parseDOLine(line: String): Option[ParsedDORecord] = {
    line match {
      case doPattern(dateStr, id, codeStr) =>
        DateTimeParser.parse(dateStr) match {
          case Some(dtParts) =>
            Some(ParsedDORecord(id, dtParts, codeStr))
          case None =>
            System.err.println(s"Failed to parse date in: ${line.take(100)}")
            None
        }
      case _ =>
        System.err.println(s"Line doesn't match DOC_OPEN pattern: ${line.take(100)}")
        None
    }
  }

  def parseDORecords(rdd: RDD[String]): RDD[ParsedDORecord] = {
    rdd.flatMap(parseDOLine)
  }
}