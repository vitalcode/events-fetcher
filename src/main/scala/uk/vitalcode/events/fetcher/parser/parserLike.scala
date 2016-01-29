package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.fetcher.model.Prop

trait ParserLike[T] {
    def parse(prop: Prop): Set[T]
}
