package org.example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.example.QSEventParser.parseQSRecords
import scala.util.matching.Regex

object QSEventParser {
  case class ParsedQSRecord(id: String, date: DateTimeParts, codes: List[String])
  private val qsPattern: Regex =
    """^QS\s+([^\s]+)\s+\{[^}]+\}\s+([\w-]*\d+)\s+((?:\w+_\d+\s*)*)""".r

  def parseQSLine(line: String): Option[ParsedQSRecord] = {
    line match {
      case qsPattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr).map { dtParts =>
          val cleanedId = if (id.startsWith("-")) id.substring(1) else id
          ParsedQSRecord(
            cleanedId,
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

object QSEventParserTest extends App {
  val testData = Seq(
    "QS 19.08.2020_01:51:34 {329-фз} 13693734 LAW_140194 LAW_304061",
    "QS 19.08.2020_01:51:34 {329-фз} -13693734 LAW_140194 LAW_304061",
    "QS Fri,_19_Aug_2020_01:51:34_+0300 {329-фз} -13693734 LAW_140194 LAW_304061",
    "QS  {329-фз} 13693734 LAW_140194 LAW_304061",
    "QS 19.08.2020_01:51:34 {329-фз} 13693734 ",
    "INVALID DATA"
  )

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
  try {
    println("="*50)
    println("Starting QSEventParser tests")
    println("="*50 + "\n")

    val rdd = sc.parallelize(testData)
    val results = parseQSRecords(rdd).collect()

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