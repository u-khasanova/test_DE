import scala.util.matching.Regex
import org.example.DateTimeParser
import org.example.DateTimeParts

val pattern: Regex =
  """^QS\s+([^\s]+)\s+\{[^}]+\}\s+(\d+)\s+((?:\w+_\d+\s*)+)""".r

def parseLine(line: String): Option[(DateTimeParts, String, List[String])] = {
  if (!line.startsWith("QS ")) {
    println(s"Не начинается с QS: ${line.take(50)}...")
    return None
  }

  val afterQS = line.drop(3).trim
  val firstBrace = afterQS.indexOf('{')
  val lastBrace = afterQS.indexOf('}')

  if (firstBrace < 0 || lastBrace < 0 || lastBrace <= firstBrace) {
    println(s"Не найдены скобки запроса: ${line.take(50)}...")
    return None
  }

  val dateStr = afterQS.substring(0, firstBrace).trim
  val query = afterQS.substring(firstBrace + 1, lastBrace).trim
  val afterQuery = afterQS.substring(lastBrace + 1).trim

  val parts = afterQuery.split("\\s+")
  if (parts.length < 2) {
    println(s"Недостаточно данных после запроса: ${line.take(50)}...")
    return None
  }

  val id = parts(0)
  val codes = parts.drop(1).toList

  DateTimeParser.parse(dateStr) match {
    case Some(dtParts) =>
      Some((dtParts, id, codes))
    case None =>
      println(s"Неизвестный формат даты: ${dateStr.take(50)}...")
      None
  }
}


val testDates = Seq(
  "01.07.2020_13:42:01",                           // dd.MM.yyyy_HH:mm:ss
  "Wed,_01_Aug_2020_13:48:07_+0300",                // EEE,_dd_MMM_yyyy_HH:mm:ss_ZZZZZ
  "2020-07-01T13:48:07+03:00",                      // ISO_OFFSET_DATE_TIME
  "2020-07-01 13:48:07",                            // yyyy-MM-dd HH:mm:ss
  "07/01/2020 01:48:07 PM"                          // MM/dd/yyyy hh:mm:ss a
)

testDates.foreach { dateStr =>
  println(s"Пробуем распарсить: $dateStr")
  DateTimeParser.parse(dateStr) match {
    case Some(parts) =>
      println(s"  ✅ Успех: дата = ${parts.date}, время = ${parts.time}")
    case None =>
      println(s"  ❌ Не удалось распарсить")
  }
}