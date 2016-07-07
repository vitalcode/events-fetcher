package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.model.Prop

// TODO could be name as URL parser
object ImageParser extends ParserLike[String] {
    override def parse(prop: Prop): Vector[String] = prop.values
}
