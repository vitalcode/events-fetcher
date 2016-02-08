package uk.vitalcode.events.fetcher.service

import java.time.format.DateTimeFormatter

import uk.vitalcode.events.fetcher.model.Prop
import uk.vitalcode.events.fetcher.model.PropType._
import uk.vitalcode.events.fetcher.parser.{ImageParser, DateParser, TextParser}

object PropertyService {

    def getFormattedValues(prop: Prop): Set[String] = {
        prop.kind match {
            case Text => TextParser.parse(prop)
            case Date =>
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
                DateParser.parse(prop).map(d => d.format(formatter))
            case Image => ImageParser.parse(prop)
            case _ => ???
        }
    }
}
