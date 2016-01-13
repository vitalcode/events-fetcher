package uk.vitalcode.events.fetcher.model

object MineType extends Enumeration {
    type MineType = Value
    val TEXT_HTML = Value("text/html") // .html
    val IMAGE_JPEG = Value("image/jpeg") // .jpeg, .jpg
    val IMAGE_PNG = Value("image/png") // .png
}
