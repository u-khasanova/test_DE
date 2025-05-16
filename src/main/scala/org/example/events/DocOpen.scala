package org.example.events

import org.apache.spark.util.LongAccumulator
import org.example.Main.{ParseError, logger}
import org.example.parser.DateTime

import scala.util.Try

case class DocOpen(
    date: DateTime,
    id: Int,
    docId: String
)

object DocOpen {
  def parse(
      line: BufferedIterator[String],
      filePath: String,
      errorAccumulator: LongAccumulator,
      sessionDate: Option[DateTime] = None
  ): Option[DocOpen] = {
    val content = line.next()
    extract(content, sessionDate) match {
      case Right(result) => Some(result)
      case Left(e) =>
        errorAccumulator.add(1)
        val err = ParseError(
          filePath,
          content,
          e.getClass.getSimpleName,
          e.getMessage,
          "DocOpen"
        )
        logger.error(err.toLogString)
        None
    }
  }

  def extract(
      content: String,
      sessionDate: Option[DateTime]
  ): Either[Throwable, DocOpen] = {
    Try {
      val parts = content.split("\\s+", -1)
      val date = Try(DateTime.parse(parts(1))).recover {
        case _ if sessionDate.isDefined => sessionDate.get
      }.get

      DocOpen(
        date = date,
        id = Try(parts(2).toInt.abs).getOrElse(0),
        docId =
          if (Try(parts(2).toInt.abs).getOrElse(0) == 0) parts(2) else parts(3)
      )
    }.toEither
  }
}
