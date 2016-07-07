package uk.vitalcode.events.fetcher.utils

import java.time.{DayOfWeek, LocalDate}

object DateTimeUtil {

    def datesInRange(fromDate: LocalDate, toDate: LocalDate, daysOfWeek: Set[DayOfWeek] = Set(), dates: Set[LocalDate] = Set()): Set[LocalDate] = {
        if (fromDate.isAfter(toDate)) dates
        else {
            datesInRange(fromDate.plusDays(1), toDate, daysOfWeek,
                if (daysOfWeek.isEmpty || daysOfWeek.contains(fromDate.getDayOfWeek)) dates + fromDate
                else dates
            )
        }
    }
}
