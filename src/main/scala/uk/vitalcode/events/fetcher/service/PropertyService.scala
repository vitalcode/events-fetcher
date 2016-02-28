package uk.vitalcode.events.fetcher.service

import uk.vitalcode.events.model.PropType._
import uk.vitalcode.events.fetcher.parser.{DateParser, ImageParser, TextParser}
import uk.vitalcode.events.model.Prop

object PropertyService {

    def getFormattedValues(prop: Prop): Vector[(String, String)] = {
        prop.kind match {
            case Text => TextParser.parse(prop).map(p => (prop.name, p)).toVector
            case Date => DateParser.parse(prop).zipWithIndex
                .flatMap {
                    case (p, 0) => Vector(("from", p))
                    case (p, 1) => Vector(("to", p))
                }
                .toVector

            case Image => ImageParser.parse(prop).map(p => (prop.name, p)).toVector
            case _ => ???
        }
    }
}

// TODO "from", "to" and others go to enum