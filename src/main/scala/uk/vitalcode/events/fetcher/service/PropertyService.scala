package uk.vitalcode.events.fetcher.service

import uk.vitalcode.events.model.PropType._
import uk.vitalcode.events.fetcher.parser.{DateParser, ImageParser, TextParser}
import uk.vitalcode.events.model.Prop
import uk.vitalcode.events.fetcher.common.Log


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

//    def getFormattedValues(prop: Prop): Vector[(String, Vector[String])] = {
//        log.info(s"Formatting values prop value [$prop]")
//        prop.kind match {
//            case Text => TextParser.parse(prop).map(p => (prop.name, Vector(p))).toVector
//            //            case Date => DateParser.parse(prop).zipWithIndex
//            //                .flatMap {
//            //                    case (p, 0) => Vector(("from", p))
//            //                    case (p, 1) => Vector(("to", p))
//            //                }
//            //                .toVector
//            case Date => DateParser.parse(prop).map(p => (prop.name, Vector(p._1, p._2))).toVector
//            case Image => ImageParser.parse(prop).map(p => (prop.name, Vector(p))).toVector
//            case _ => ???
//        }
//    }
}

//object PropertyService extends Log {
//
//    def getFormattedValues(prop: Prop): Vector[(String, String)] = {
//        log.info(s"Formatting values prop value [$prop]")
//        prop.kind match {
//            case Text => TextParser.parse(prop).map(p => (prop.name, p)).toVector
//            case Date => DateParser.parse(prop).zipWithIndex
//                .flatMap {
//                    case (p, 0) => Vector(("from", p))
//                    case (p, 1) => Vector(("to", p))
//                }
//                .toVector
//
//            case Image => ImageParser.parse(prop).map(p => (prop.name, p)).toVector
//            case _ => ???
//        }
//    }
//}

// TODO "from", "to" and others go to enum