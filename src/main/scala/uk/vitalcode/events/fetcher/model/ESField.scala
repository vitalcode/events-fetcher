package uk.vitalcode.events.fetcher.model

object EsField extends Enumeration {
    type ESField = Value
    val TEXT_HTML = Value("text/html") // .html
    val IMAGE_JPEG = Value("image/jpeg") // .jpeg, .jpg
    val IMAGE_PNG = Value("image/png") // .png
}