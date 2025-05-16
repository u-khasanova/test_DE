package org.example.events

import org.apache.spark.util.LongAccumulator
import org.example.Main.{ParseError, logger}
import org.example.parser.DateTime

import scala.collection.mutable
import scala.util.Try

case class CardSearch(
    date: DateTime,
    id: Int,
    query: String,
    docIds: List[String]
)

object CardSearch {

  def parse(
      lines: BufferedIterator[String],
      filePath: String,
      errorAccumulator: LongAccumulator,
      sessionDate: Option[DateTime] = None
  ): Option[CardSearch] = {
    extract(lines, sessionDate) match {
      case Right(result) => Some(result)
      case Left((content, e)) =>
        errorAccumulator.add(1)
        val err = ParseError(
          filePath,
          content,
          e.getClass.getSimpleName,
          e.getMessage,
          "CardSearch"
        )
        logger.error(err.toLogString)
        None
    }
  }

  def extract(
      lines: BufferedIterator[String],
      sessionDate: Option[DateTime]
  ): Either[(String, Throwable), CardSearch] = {
    val content = new mutable.StringBuilder(lines.next().trim)
    var foundEnd = false

    while (lines.hasNext && !foundEnd) {
      val nextLine = lines.next()
      content.append(s" $nextLine")
      foundEnd = nextLine.trim.startsWith("CARD_SEARCH_END")
      if (foundEnd) content.append(s" ${lines.next()}")
    }

    val fullContent = content.toString()

    Try {
      val parts = fullContent.split("CARD_SEARCH_END")
      val beforeEnd = parts(0).trim.split("\\s+", 3)
      val afterEnd = parts(1).trim.split("\\s+")

      val date = Try(DateTime.parse(beforeEnd(1))).recover {
        case _ if sessionDate.isDefined => sessionDate.get
      }.get

      CardSearch(
        date = date,
        id = Try(afterEnd.head.toInt.abs).getOrElse(0),
        query = beforeEnd(2),
        docIds =
          if (Try(afterEnd.head.toInt.abs).getOrElse(0) == 0) afterEnd.toList
          else afterEnd.tail.toList
      )
    }.toEither.left.map(e => (fullContent, e))
  }
}
