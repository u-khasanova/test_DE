package org.example.processors

import org.apache.spark.util.CollectionAccumulator
import org.example.Main.ParseError
import org.example.events.Session

import scala.collection.mutable

object RawDataProcessor {

  case class ParseContext(
      filePath: String,
      iterator: BufferedIterator[String],
      var currentSession: SessionBuilder,
      sessions: mutable.ListBuffer[SessionBuilder],
      recoverId: Boolean,
      recoverEmptyDate: Boolean
  )

  def process(
      content: String,
      filePath: String,
      errorAccumulator: CollectionAccumulator[String],
      recoverId: Boolean,
      recoverEmptyDate: Boolean
  ): Iterator[Session] = {

    val lines = content
      .split("\n")
      .iterator
      .buffered

    val context = ParseContext(
      filePath = filePath,
      iterator = lines,
      currentSession = SessionBuilder(filePath),
      sessions = mutable.ListBuffer.empty,
      recoverId = recoverId,
      recoverEmptyDate = recoverEmptyDate
    )

    try {
      Session.parse(context)
    } catch {
      case e: Exception =>
        val err = ParseError(
          filePath,
          context.iterator.head,
          s"${e.getStackTrace.head}",
          e.getClass.getSimpleName,
          e.getMessage
        )
        errorAccumulator.add(err.toLogString)
    }
    context.sessions.iterator.flatMap(_.build(context))
  }
}
