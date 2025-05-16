package org.example.events

import org.apache.spark.util.LongAccumulator
import org.example.Main.{ParseError, logger}
import org.example.parser.DateTime
import scala.util.Try

case class QuickSearch(
    date: DateTime,
    id: Int,
    query: String,
    docIds: List[String]
)

object QuickSearch {

  def parse(
      lines: BufferedIterator[String],
      filePath: String,
      errorAccumulator: LongAccumulator,
      sessionDate: Option[DateTime] = None
  ): Option[QuickSearch] = {
    val line = lines.next().trim
    val nextLine = lines.next().trim
    val content = s"$line $nextLine"

    extract(content, sessionDate) match {
      case Right(result) => Some(result)
      case Left(e) =>
        errorAccumulator.add(1)
        val err = ParseError(
          filePath,
          content,
          e.getClass.getSimpleName,
          e.getMessage,
          "QuickSearch"
        )
        logger.error(err.toLogString)
        None
    }
  }

  def extract(
      content: String,
      sessionDate: Option[DateTime] = None
  ): Either[Throwable, QuickSearch] = {
    Try {
      val openBrace = content.indexOf('{')
      val closeBrace = content.indexOf('}', openBrace + 1)

      val date = Try {
        DateTime.parse(
          content
            .substring(0, openBrace)
            .split("\\s+")
            .last
        )
      }.recover {
        case _ if sessionDate.isDefined => sessionDate.get
      }.get

      val query = content
        .substring(openBrace + 1, closeBrace)
        .trim

      val parts = content
        .substring(closeBrace + 1)
        .trim
        .split("\\s+")

      val id = Try(parts.head.toInt.abs).getOrElse(0)
      val docIds = if (id == 0) parts.toList else parts.tail.toList

      QuickSearch(date, id, query, docIds)
    }.toEither
  }
}
