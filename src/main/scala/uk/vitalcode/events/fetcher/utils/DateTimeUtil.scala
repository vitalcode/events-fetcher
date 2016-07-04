package uk.vitalcode.events.fetcher.utils

import java.time.LocalDate

object DateTimeUtil {

    def datesInRange(fromDate: LocalDate, toDate: LocalDate, dates: Set[LocalDate] = Set()): Set[LocalDate] = {
        if (fromDate.isAfter(toDate)) dates
        else datesInRange(fromDate.plusDays(1), toDate, dates + fromDate)
    }
}
