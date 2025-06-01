package org.example.processor.utils

case class ParseWarning(
    filePath: String,
    line: String,
    methodName: String,
    fieldName: String
) {
  def toParseError: ParseError =
    ParseError(
      filePath,
      line,
      methodName,
      "EmptyFieldError",
      s"Field '$fieldName' is empty or invalid"
    )
}
