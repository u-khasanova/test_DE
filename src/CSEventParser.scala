package org.example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.example.CSEventParser.parseCSRecords

import scala.util.matching.Regex

object CSEventParser {
  case class ParsedCSRecord(id: String, date: DateTimeParts, codes: List[String])
  private val csPattern: Regex =
    """^CARD_SEARCH_START\s+([^\s]+).*?CARD_SEARCH_END\s+([\w-]*\d+)\s+((?:\w+_\d+\s*)*)""".r

  def parseCSLine(line: String): Option[ParsedCSRecord] = {
    line match {
      case csPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr).map { dtParts =>
          val cleanedId = if (id.startsWith("-")) id.substring(1) else id
          ParsedCSRecord(
            cleanedId,
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

object CSEventParserTest extends App {
  val testData = Seq(
    "CARD_SEARCH_START 19.08.2020_01:48:39 $134 возврат некачественного товара поставщику права потребителей CARD_SEARCH_END 32181015 PBI_259700 PBI_236500 PBI_258514 PBI_236312",
    "CARD_SEARCH_START 19.08.2020_01:48:39 $134 возврат некачественного товара поставщику права потребителей CARD_SEARCH_END -32181015 PBI_259700 PBI_236500 PBI_258514 PBI_236312",
    "CARD_SEARCH_START Fri,_19_Aug_2020_01:51:34_+0300 $134 возврат некачественного товара поставщику права потребителей CARD_SEARCH_END -32181015 PBI_259700 PBI_236500 PBI_258514 PBI_236312",
    "CARD_SEARCH_START   $134 возврат некачественного товара поставщику права потребителей CARD_SEARCH_END -32181015 PBI_259700 PBI_236500 PBI_258514 PBI_236312",
    "CARD_SEARCH_START 19.08.2020_01:48:39 $134 возврат некачественного товара поставщику права потребителей CARD_SEARCH_END 32181015 ",
    "INVALID DATA"
  )

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
  try {
    println("="*50)
    println("Starting CSEventParser tests")
    println("="*50 + "\n")

    val rdd = sc.parallelize(testData)
    val results = parseCSRecords(rdd).collect()

    testData.zipWithIndex.foreach { case (record, idx) =>
      println(s"Record #${idx + 1}: ${record}")
      println("-"*40)
    }

    println(s"Succesfully parsed records: ${results.length}\n")

    results.zipWithIndex.foreach { case (record, idx) =>
      println(s"Record #${idx + 1}:")
      println(s"ID: ${record.id}")
      println(s"Date: ${record.date.date} ${record.date.time}")
      println(s"Codes: ${record.codes.mkString(", ")}")
      println("-"*40)
    }

    val invalidCount = testData.size - results.length
    println(s"\nParsing is failed $invalidCount times")
  } finally {
    sc.stop()
  }
}