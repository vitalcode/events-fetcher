package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.fetcher.model.Prop

object TextParser extends ParserLike[String] {
    override def parse(prop: Prop): Set[String] = prop.values
}
