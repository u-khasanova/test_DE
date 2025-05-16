package org.example.events

import org.example.parser.DateTime

case class Session(
    startDate: DateTime,
    endDate: DateTime,
    quickSearches: List[QuickSearch],
    cardSearches: List[CardSearch],
    docOpens: List[DocOpen]
)
