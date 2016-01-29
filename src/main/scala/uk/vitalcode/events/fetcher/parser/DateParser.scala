package uk.vitalcode.events.fetcher.parser

import java.time.LocalDateTime

import uk.vitalcode.events.fetcher.model.Prop

object DateParser extends ParserLike[LocalDateTime] {
    override def parse(prop: Prop): Set[LocalDateTime] = Set[LocalDateTime](LocalDateTime.now())
}