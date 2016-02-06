package uk.vitalcode.events.fetcher.common

object StringUtils {
    implicit class StringExtensions(string: String) {
        def stripForJson: String = {
            string.replaceAll("[\\s|]", "")
        }
    }
}
