package org.example.errorProcessors

case class ParseError(
    filePath: String,
    line: String,
    methodName: String,
    errorType: String,
    errorMessage: String
)
