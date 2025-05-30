package org.example.errorProcessors

import org.example.processors.RawDataProcessor.ParseContext

object EmptyFieldLogger {

  def log(
      context: ParseContext,
      fieldName: String,
      value: Option[_],
      line: String,
      method: String
  ): Unit = {
    if (value.isEmpty) {
      context.errorAccumulator.add(
        ParseError(
          context.filePath,
          line,
          method,
          "EmptyFieldError",
          s"Field '$fieldName' is empty or invalid"
        )
      )
    }
  }
}
