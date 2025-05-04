package org.example.events
import org.example.parser.DateTimeParts
import java.time.Duration
import java.time.LocalDateTime

case class Session(
                    startDate: DateTimeParts,
                    endDate: DateTimeParts,
                    QS: List[QS],
                    cardSearch: List[CardSearch],
                    docOpen: List[DocOpen]
                  )