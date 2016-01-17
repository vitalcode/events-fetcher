package uk.vitalcode.events.fetcher.model

trait Builder {
    type t

    def build(): t
}
