package org.example.processor.utils

import org.example.processor.SessionBuilder

case class ParseContext(
    filePath: String,
    iterator: BufferedIterator[String],
    var currentSession: SessionBuilder,
    errorAccumulator: ErrorsAccumulator
) {
  def addError(e: Exception): Unit = {
    val err = ParseError(
      filePath,
      iterator.head,
      s"${e.getStackTrace.head}",
      e.getClass.getSimpleName,
      e.getMessage
    )
    errorAccumulator.add(err)
  }

  def addWarning(methodName: String, fieldName: String): Unit = {
    val warning = ParseWarning(
      filePath,
      iterator.head,
      methodName,
      fieldName
    )
    errorAccumulator.add(warning.toParseError)
  }
}
