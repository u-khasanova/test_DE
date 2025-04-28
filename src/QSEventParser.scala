package org.example
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object QSEventParser {
  case class ParsedQSRecord(id: String, date: DateTimeParts, codes: List[String])
  private val qsPattern: Regex =
    """^QS\s+([^\s]+)\s+\{[^}]+\}\s+([\w-]*\d+)\s+((?:\w+_\d+\s*)*)""".r

  def parseQSLine(line: String): Option[ParsedQSRecord] = {
    line match {
      case qsPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr).map { dtParts =>
          ParsedQSRecord(
            id,
            dtParts,
            if (codesStr.trim.isEmpty) Nil else codesStr.trim.split("\\s+").toList
          )
        }
      case _ => None
    }
  }
  def parseQSRecords(rdd: RDD[String]): RDD[ParsedQSRecord] = {
    rdd.mapPartitions(_.flatMap(parseQSLine))
  }
}

//val testData = Seq(
//      "QS 19.08.2020_01:51:34 {329-фз} 13693734 LAW_140194 LAW_304061 LAW_189522 LAW_359169 LAW_334459 LAW_170478 LAW_201517",
//      "QS 19.08.2020_01:51:34 {329-фз} -13693734 LAW_140194 LAW_304061 LAW_189522 LAW_359169 LAW_334459 LAW_170478 LAW_201517",
//      "QS Fri,_19_Aug_2020_01:51:34_+0300 {329-фз} -13693734 LAW_140194 LAW_304061 LAW_189522 LAW_359169 LAW_334459 LAW_170478 LAW_201517",
//      "QS  {329-фз} 13693734 LAW_140194 LAW_304061 LAW_189522 LAW_359169 LAW_334459 LAW_170478 LAW_201517",
//      "QS 19.08.2020_01:51:34 {329-фз} 13693734 "
//)
