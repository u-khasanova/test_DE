package org.example.processors

import org.example.errorProcessors.ErrorAccumulator
import org.example.events.Session

import scala.collection.mutable

object RawDataProcessor {

  case class ParseContext(
      filePath: String,
      iterator: BufferedIterator[String],
      var currentSession: SessionBuilder,
      sessions: mutable.ListBuffer[SessionBuilder],
      recoverId: Boolean,
      recoverEmptyDate: Boolean,
      errorAccumulator: ErrorAccumulator
  )

  def process(
      content: String,
      filePath: String,
      errorAccumulator: ErrorAccumulator,
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
      recoverEmptyDate = recoverEmptyDate,
      errorAccumulator = errorAccumulator
    )

    Session.parse(context)
    context.sessions.iterator.flatMap(_.build(context))
  }
}
