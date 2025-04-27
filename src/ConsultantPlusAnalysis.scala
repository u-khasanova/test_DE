import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts
import org.apache.spark.internal.Logging
import scala.util.matching.Regex

object ConsultantPlusAnalysis extends Logging {
  def filterQS(rdd: RDD[String]): RDD[String] = {
    rdd.filter(line => line.startsWith("QS"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd = sc.textFile("src/main/resources/data/*").cache()
    val indexedRDD = rdd.zipWithIndex()

    // Обработка событий быстрого поиска
    val qsIndices = indexedRDD
      .collect { case (line, index) if line.startsWith("QS") => index }
      .collect()
      .toSet

    val qsWithNext = indexedRDD
      .filter { case (line, index) =>
        line.startsWith("QS") || qsIndices.contains(index - 1)
      }
      .map(_._1)

    val pattern: Regex =
      """^QS\s+([^\s]+)\s+\{[^}]+\}\s+(\d+)\s+((?:\w+_\d+\s*)+)""".r

    def parseLine(line: String): Option[(DateTimeParts, String, List[String])] = {
      line match {
        case pattern(dateStr, id, codesStr) =>
          DateTimeParser.parse(dateStr) match {
            case Some(dtParts) =>
              val codes = codesStr.trim.split("\\s+").toList
              Some((dtParts, id, codes))
            case None =>
              logWarning(s"Неизвестный формат даты: ${dateStr.take(50)}...")
              None
          }
        case _ =>
          logWarning(s"Не распознано: ${line.take(50)}...")
          None
      }
    }

    // Пример обработки с логированием
    qsWithNext.collect().foreach { line =>
      parseLine(line).getOrElse {
        logDebug(s"Строка не была обработана: ${line.take(100)}")
      }
    }

    spark.stop()
  }
}