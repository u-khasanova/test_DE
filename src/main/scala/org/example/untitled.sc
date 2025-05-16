import org.apache.spark.util.LongAccumulator
import org.example.parser.SessionParser

val errorAccumulator = new LongAccumulator

val content = """
                |SESSION_START 08.11.2020_12:29:47
                |QS 08.11.2020_12:30:36 {query}
                |123 DOC1
                |CARD_SEARCH_START 08.11.2020_12:31:00
                |search query
                |CARD_SEARCH_END
                |456 DOC2 DOC3
                |DOC_OPEN 08.11.2020_12:32:00 123 DOC1
                |SESSION_END 08.11.2020_12:33:00
                |""".stripMargin
val result = SessionParser.parse(content, "test.log", errorAccumulator)

result.isDefined
result.foreach { session =>
  println(session.quickSearches)
  println(session.cardSearches)
  println(session.docOpens)
  println(session.startDate.toString == "08.11.2020 12:29:47")
  println(session.endDate.toString == "08.11.2020 12:33:00")
}
