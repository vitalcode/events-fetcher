package uk.vitalcode.events.fetcher.service

import uk.vitalcode.events.model.PropType._
import uk.vitalcode.events.fetcher.parser.{ImageParser, TextParser}
import uk.vitalcode.events.model.Prop
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.parser.dateParser.DateParser


object PropertyService extends Log {

    def getFormattedValues(prop: Prop): Vector[(String, Any)] = {
        log.info(s"Formatting values prop value [$prop]")
        prop.kind match {
            case Text => TextParser.parse(prop).map(p => (prop.name, p)).toVector
            case Date => DateParser.parse(prop).map(p => (prop.name, Vector(p._1, p._2))).toVector
            case Image => ImageParser.parse(prop).map(p => (prop.name, p)).toVector
            case _ => ???
        }
    }
}