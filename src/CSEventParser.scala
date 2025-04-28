package org.example
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

object CSEventParser {
  case class ParsedCSRecord(id: String, date: DateTimeParts, codes: List[String])
  private val csPattern: Regex =
    """^CARD_SEARCH_START\s+([^\s]+).*?CARD_SEARCH_END\s+([\w-]*\d+)\s+((?:\w+_\d+\s*)*)""".r

  def parseCSLine(line: String): Option[ParsedCSRecord] = {
    line match {
      case csPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr).map { dtParts =>
          ParsedCSRecord(
            id,
            dtParts,
            if (codesStr.trim.isEmpty) Nil else codesStr.trim.split("\\s+").toList
          )
        }
      case _ => None
    }
  }

  def parseCSRecords(rdd: RDD[String]): RDD[ParsedCSRecord] = {
    rdd.mapPartitions(_.flatMap(parseCSLine))
  }
}