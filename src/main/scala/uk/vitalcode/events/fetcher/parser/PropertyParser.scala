package uk.vitalcode.events.fetcher.parser

import java.time.LocalDateTime

import shapeless.Poly1
import uk.vitalcode.events.fetcher.model.Prop

trait PropertyParser[T] {
    def parse(p: Prop[T]): Prop[T]
}

object PropertyParser extends Poly1 {
    implicit def propertyString = at[Prop[String]](TextParser.parse)

    implicit def propertyDate = at[Prop[(LocalDateTime, LocalDateTime)]](DateParser.parse)
}



