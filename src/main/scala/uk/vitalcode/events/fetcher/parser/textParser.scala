package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.fetcher.model.Prop

object TextParser extends PropertyParser[String] {
    override def parse(p: Prop[String]): Prop[String] =
        p.copy(value = p.raw.map(r => r.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")))
}

