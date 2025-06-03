package org.example.processor.utils

case class ParseWarning(
    filePath: String,
    line: String,
    methodName: String = "",
    fieldName: String = ""
) {
  def toEmptyFieldError: ParseError =
    ParseError(
      filePath,
      line,
      methodName,
      "EmptyFieldError",
      s"Field '$fieldName' is empty or invalid"
    )

  def toTrailingLineAfterSessionEndError: ParseError =
    ParseError(
      filePath,
      line,
      "Session.parse",
      "TrailingLineAfterSessionEndError",
      "Unexpected line after SESSION_END"
    )
}
