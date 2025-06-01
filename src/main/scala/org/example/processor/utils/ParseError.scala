package org.example.processor.utils

case class ParseError(
    filePath: String,
    line: String,
    methodName: String,
    errorType: String,
    errorMessage: String
)
