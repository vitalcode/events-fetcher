package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.model.Prop

object TextParser extends ParserLike[String] {
    val notAllowedCharasters: String = """[^\w ,.\/\\<>?!|"'’`:;{}\[\]=+-_()@$#£%^&*~±]+"""

    override def parse(prop: Prop): Set[String] = prop.values
        .map(_.replaceAll(notAllowedCharasters, " ")
            // CamelCase to Camel Case Text: http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
            .replaceAll("""(?<=[A-Z])(?=[A-Z][a-z])|(?<=[^A-Z0-9,.\/\\<>?!|"'’`:;{}\[\]=+-_()@$#£%^&*~±])(?=[A-Z])""", " ")
            .replaceAll( """\s{2,}""", " ")
            .replaceAll( """^\s|\s$""", "")
        )
        .filter(_.nonEmpty)
}
