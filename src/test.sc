import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.example.DateTimeParser
import org.example.DateTimeParts

object ConsultantPlusAnalysis {
  case class QSRecord(id: String, date: DateTimeParts, codes: List[String])

  def main(args: Array[String]): Unit = {
    // Настраиваем Spark с выводом INFO в консоль
    val spark = SparkSession.builder()
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.showConsoleProgress", "true")
      .getOrCreate()

    // Устанавливаем уровень логирования
    spark.sparkContext.setLogLevel("INFO")

    try {
      val sc = spark.sparkContext

      println("=== Загрузка данных ===")
      val inputRDD = sc.textFile("src/main/resources/data/0")
      println(s"Загружено строк: ${inputRDD.count()}")

      println("\n=== Поиск QS запросов ===")
      val qsIndices = inputRDD.zipWithIndex()
        .filter { case (line, _) => line.startsWith("QS") }
        .map { case (_, idx) => idx }
        .collect()
        .toSet
      println(s"Найдено QS запросов: ${qsIndices.size}")

      val qsLinesWithNext = inputRDD.zipWithIndex()
        .filter { case (_, idx) => qsIndices.contains(idx) || qsIndices.contains(idx - 1) }
        .map { case (line, _) => line }

      println("\n=== Парсинг результатов ===")
      val parsedResults = parseQSRecords(qsLinesWithNext)
      showSampleResults(parsedResults)

    } finally {
      spark.stop()
    }
  }

  private object QSParser extends Serializable {
    private val pattern = """^QS\s+([^\s]+)\s+\{[^}]+\}\s+(\d+)\s+((?:\w+_\d+\s*)+)""".r

    def parse(line: String): Option[QSRecord] = line match {
      case pattern(dateStr, id, codesStr) =>
        DateTimeParser.parse(dateStr) match {
          case Some(dtParts) =>
            Some(QSRecord(id, dtParts, codesStr.trim.split("\\s+").toList))
          case None =>
            println(s"Неизвестный формат даты: ${dateStr.take(50)}")
            None
        }
      case _ => None
    }
  }

  private def parseQSRecords(rdd: RDD[String]): RDD[QSRecord] = {
    rdd.flatMap(QSParser.parse)
  }

  private def showSampleResults(results: RDD[QSRecord]): Unit = {
    println("\n=== Примеры записей ===")
    results.take(5).foreach { record =>
      println(s"ID: ${record.id}, Дата: ${record.date}, Результаты: ${record.codes.mkString(", ")}")
    }
    println(s"\nВсего распознано записей: ${results.count()}")
  }
}