package org.example.processor.utils

import org.example.processor.SessionBuilder

import scala.collection.mutable

case class ParseContext(
    filePath: String,
    iterator: BufferedIterator[String],
    currentSession: SessionBuilder,
    errors: mutable.ListBuffer[ParseError] = mutable.ListBuffer.empty
) {
  def addError(e: Exception): Unit = {
    val currentLine = if (iterator.hasNext) iterator.head else "[END OF FILE]"
    val err = ParseError(
      filePath,
      currentLine,
      s"${e.getStackTrace.head}",
      e.getClass.getSimpleName,
      e.getMessage
    )
    errors += err
  }

  def addEmptyFieldWarning(methodName: String, fieldName: String): Unit = {
    val currentLine = if (iterator.hasNext) iterator.head else "[END OF FILE]"
    val warning = ParseWarning(
      filePath,
      currentLine,
      methodName,
      fieldName
    )
    errors += warning.toEmptyFieldError
  }

  def addTrailingLineAfterSessionEndWarning(): Unit = {
    val currentLine = if (iterator.hasNext) iterator.head else "[END OF FILE]"
    val warning = ParseWarning(
      filePath,
      currentLine
    )
    errors += warning.toTrailingLineAfterSessionEndError
  }
}
