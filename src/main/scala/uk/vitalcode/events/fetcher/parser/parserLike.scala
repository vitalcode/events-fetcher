package uk.vitalcode.events.fetcher.parser

trait ParserLike[T] {
    def parse(text: String): T
}
