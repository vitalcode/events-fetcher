package uk.vitalcode.events.fetcher.model

trait Builder extends Serializable {
    type t

    def build(): t
}
