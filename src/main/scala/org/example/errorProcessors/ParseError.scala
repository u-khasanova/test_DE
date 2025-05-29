package org.example.errorProcessors

case class ParseError(
    filePath: String,
    line: String,
    methodName: String,
    errorType: String,
    errorMessage: String
) {
  def toLogString: String = {
    s"$errorType in $filePath using $methodName: $errorMessage" +
      (if (line.nonEmpty) s"\nProblem line: $line" else "")
  }
}
