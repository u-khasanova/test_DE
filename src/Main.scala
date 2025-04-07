import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object ConsultantPlusAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ConsultantPlusAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDir = "src/main/resources/data/0"
    val lines = spark.read.textFile(inputDir).rdd

    // Объединяем строки по парам: QS (поисковый запрос) и список документов
    val parsed = lines.zipWithIndex().map { case (line, idx) => (idx, line) }
    val byIndex = parsed.collect().toMap

    val data = parsed.flatMap { case (idx, line) =>
      val parts = line.split(" ", 3)
      if (line.startsWith("QS")) {
        val ts = parts(1)
        val queryText = parts.lift(2).getOrElse("")
        val nextLine = byIndex.getOrElse(idx + 1, "")
        val nextParts = nextLine.split(" ")
        if (nextParts.length >= 2) {
          val qsId = nextParts(0)
          val documents = nextParts.drop(1).toList
          Some(("QS", ts, qsId, queryText, documents.mkString(" ")))
        } else None
      } else if (line.startsWith("DOC_OPEN")) {
        val ts = parts(1)
        val tokens = parts(2).split(" ")
        if (tokens.length >= 2) {
          val qsId = tokens(0)
          val docId = tokens(1)
          Some(("DOC_OPEN", ts, qsId, "", docId))
        } else None
      } else None
    }

    val df = data.toDF("event_type", "timestamp", "qs_id", "query_text", "document_data")

    // 1. Сколько раз был найден ACC_45616
    val qsDf = df.filter($"event_type" === "QS")
      .withColumn("documents", split(col("document_data"), " "))

    val accCount = qsDf.filter(array_contains(col("documents"), "ACC_45616")).count()
    println(s"Количество раз, когда в карточке производили поиск документа ACC_45616: $accCount")

    // 2. Кол-во открытий каждого документа, найденного через быстрый поиск, по дням
    val docOpenDf = df.filter($"event_type" === "DOC_OPEN")
      .withColumnRenamed("document_data", "document_id")
      .withColumn("date", to_date(to_timestamp($"timestamp", "dd.MM.yyyy_HH:mm:ss")))

    val qsDocMap = qsDf.select($"qs_id", $"documents")

    val joined = docOpenDf.join(qsDocMap, Seq("qs_id"))
      .filter(array_contains(col("documents"), col("document_id")))
      .groupBy("date", "document_id")
      .count()
      .orderBy("date", "document_id")

    joined.show(truncate = false)

    spark.stop()
  }
}