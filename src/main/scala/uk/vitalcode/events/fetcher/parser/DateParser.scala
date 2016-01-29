package uk.vitalcode.events.fetcher.parser

import java.time.LocalDateTime

import uk.vitalcode.events.fetcher.model.Prop

object DateParser extends PropertyParser[(LocalDateTime, LocalDateTime)] {
    override def parse(p: Prop[(LocalDateTime, LocalDateTime)]): Prop[(LocalDateTime, LocalDateTime)] =
        p.copy(value = Set[(LocalDateTime, LocalDateTime)]((LocalDateTime.of(2016, 1, 27, 0, 0, 0), LocalDateTime.of(2016, 6, 27, 0, 0, 0))))
}

