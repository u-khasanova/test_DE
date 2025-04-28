package org.example
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
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
            case Some(dtParts) => Right(ParsedDORecord(id, dtParts, codeStr))
            case None => Left(ParseError(line.take(100), s"Invalid date format: $dateStr"))
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