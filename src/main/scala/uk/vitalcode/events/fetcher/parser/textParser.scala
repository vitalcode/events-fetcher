package uk.vitalcode.events.fetcher.parser

import uk.vitalcode.events.model.Prop

object TextParser extends ParserLike[String] {
    val notAllowedCharasters: String = """[^\w ,.\/\\<>?!|"'’‘`:;{}\[\]=+-_()@$#£%^&*~±]+"""

    override def parse(prop: Prop): Vector[String] = prop.values
        .map(value => {
            var normalised = value.replaceAll(notAllowedCharasters, " ")
            if (prop.name == "venue") {
                // CamelCase to Camel Case Text: http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
                normalised = normalised.replaceAll("""(?<=[A-Z])(?=[A-Z][a-z])|(?<=[^A-Z0-9,.\/\\<>?!|"'’‘`:;{}\[\]=+-_()@$#£%^&*~±])(?=[A-Z])""", " ")
            }
            normalised.replaceAll( """(\s{2,})|(_{2,})""", " ").replaceAll( """^\s|\s$""", "")
        })
        .filter(_.nonEmpty)
}
