package uk.vitalcode.events.fetcher.model

import uk.vitalcode.events.fetcher.model.PropType.PropType

object PropType extends Enumeration with Serializable {
    type PropType = Value
    val Text, Date, Link = Value
}

case class Prop[T](name: String, css: String, kind: PropType, raw: Set[String] = Set.empty[String], value: Set[T] = Set.empty[T]) extends Serializable {

    def reset(): Prop[T] = {
        this.copy(raw = Set.empty[String], value = Set.empty[T])
    }

    def setName(name: String): Prop[T] = copy(name = name)

    def setCss(css: String): Prop[T] = copy(css = css)

    def setKind(kind: PropType): Prop[T] = copy(kind = kind)

    def setRaw(raw: Set[String]): Prop[T] = copy(raw = raw)
}

object Prop {
    def apply[T]() = new Prop[T](null, null, null)
}

//case class PropBuilder[T]() {
//    private var name: String = _
//    private var css: String = _
//    private var kind: PropType = _
//    private var raw: Set[String] = _
//
//    def setName(name: String): PropBuilder[T] = {
//        this.name = name
//        this
//    }
//
//    def setCss(css: String): PropBuilder[T] = {
//        this.css = css
//        this
//    }
//
//    def setKind(kind: PropType): PropBuilder[T] = {
//        this.kind = kind
//        this
//    }
//
//    def setRaw(raw: Set[String]): PropBuilder[T] = {
//        this.raw = raw
//        this
//    }
//
//    def build(): Prop[T] = new Prop[T](name, css, kind, raw)
//}