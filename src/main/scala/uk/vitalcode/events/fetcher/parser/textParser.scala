package uk.vitalcode.events.fetcher.parser

object TextParser extends ParserLike[String] {
    override def parse(text: String): String = text.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")
}
