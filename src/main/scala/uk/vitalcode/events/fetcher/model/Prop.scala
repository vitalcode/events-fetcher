package uk.vitalcode.events.fetcher.model

import uk.vitalcode.events.fetcher.model.PropType.PropType

object PropType extends Enumeration with Serializable {
    type PropType = Value
    val Text, Date, Image = Value
}

case class Prop(name: String, css: String, kind: PropType, values: Set[String]) extends Serializable

case class PropBuilder() extends Builder {
    private var name: String = _
    private var css: String = _
    private var kind: PropType = _

    def setName(name: String): PropBuilder = {
        this.name = name
        this
    }

    def setCss(css: String): PropBuilder = {
        this.css = css
        this
    }

    def setKind(kind: PropType): PropBuilder = {
        this.kind = kind
        this
    }

    override type t = Prop

    override def build(): Prop = new Prop(name, css, kind, Set.empty[String])
}