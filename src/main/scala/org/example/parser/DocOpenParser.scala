package org.example
package parser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.example.events.DocOpen

object DocOpenParser {
  def parse(line: String): Option[DocOpen] = {
    val trimmed = line.split(" ")
    val date = DateTimeParser.parse(trimmed(1)).getOrElse(return None)
    val id = trimmed(2).replaceAll("[^0-9]", "")
    if (id.isEmpty) return None
    val docId = trimmed(3)

    Some(DocOpen(date, id, docId))
  }
}

//object DOEventParserTest extends App {
//  val testData = Seq(
//    "DOC_OPEN 19.08.2020_01:51:34 13693734 LAW_140194",
//    "DOC_OPEN  13693734 LAW_140194",
//    "DOC_OPEN invalid_date 13693734 LAW_140194",
//    "DOC_OPEN 19.08.2020_01:51:34 -13693734 LAW_140194",
//    "INVALID FORMAT LINE",
//    "DOC_OPEN 19.08.2020_01:51:34 13693734",
//    "DOC_OPEN 2020-08-19T01:51:34 13693734 LAW_140194"
//  )
//
//  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
//  try {
//    println("="*50)
//    println("Starting DOEventParser tests")
//    println("="*50 + "\n")
//
//    println("Тesting parseDOLine:")
//    testData.foreach { line =>
//      println(s"\nRecord: ${line.take(60)}")
//      parseDOLine(line) match {
//        case Right(record) =>
//          println("Successfully parsed:")
//          println(s"ID: ${record.id}")
//          println(s"Date: ${record.date.date} ${record.date.time}")
//          println(s"Code: ${record.code}")
//        case Left(error) =>
//          println(s"Error: ${error.error}")
//          println(s"Context: ${error.rawLine}")
//      }
//    }
//
//    println("\n" + "="*50)
//    println("Testing parseDORecords (RDD):")
//    val rdd = sc.parallelize(testData)
//    val (successRecords, errorLogs) = parseDORecords(rdd)
//
//    println(s"\nSuccessfully parsed: ${successRecords.count()}")
//    successRecords.take(3).foreach { record =>
//      println(s"ID: ${record.id}, Дата: ${record.date.date}, Код: ${record.code}")
//    }
//
//    println(s"\nParse error count: ${errorLogs.count()}")
//    errorLogs.take(3).foreach { error =>
//      println(s"Error: ${error.error}")
//      println(s"Context: ${error.rawLine}")
//    }
//  } finally {
//    sc.stop()
//  }
//}