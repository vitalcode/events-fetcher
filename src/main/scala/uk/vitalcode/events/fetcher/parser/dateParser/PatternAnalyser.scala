package uk.vitalcode.events.fetcher.parser.dateParser

import java.time.{DayOfWeek, LocalDate, LocalDateTime, LocalTime}

import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.utils.DateTimeUtil

object PatternAnalyser extends Log {

    val everyDayEventMaxRepetition = 30

    // todo dates should be List[LocalDate]
    def apply(dates: Vector[LocalDate],
              times: Vector[LocalTime],
              daysOfWeek: Set[DayOfWeek],
              dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)],
              range: Boolean): Set[(LocalDateTime, Option[LocalDateTime])] = {

        dates.toList match {
            case Nil =>
                log.info("No dates")
                Set()
            case date :: Nil => analyseOneDatePattern(date, times, range)
            case fromDate :: toDate :: Nil => analyseDateRangePattern(fromDate, toDate, times, daysOfWeek, dayOfWeekTimes)
            case _ => analyseMultipleDatesPattern(dates, times)
        }
    }

    def analyseOneDatePattern(date: LocalDate,
                              times: Vector[LocalTime],
                              range: Boolean): Set[(LocalDateTime, Option[LocalDateTime])] = {

        if (range || times.isEmpty) {
            val fromDate = dateWithFromTime(date, times.headOption)
            val toDate = dateWithToTime(date, times.lastOption)
            Set((fromDate, if (fromDate != toDate) Some(toDate) else None))
        } else {
            times.map(t => (dateWithFromTime(date, Some(t)), None)).toSet
        }
    }


    def analyseDateRangePattern(fromDate: LocalDate,
                                toDate: LocalDate,
                                times: Vector[LocalTime],
                                daysOfWeek: Set[DayOfWeek],
                                dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)]): Set[(LocalDateTime, Option[LocalDateTime])] = {

        val dates: Set[LocalDate] = DateTimeUtil.datesInRange(fromDate, toDate, daysOfWeek)

        if (dates.size > everyDayEventMaxRepetition) Set()
        else dates.map(d => (
            dateWithFromTime(d, times.headOption, dayOfWeekTimes),
            Some(dateWithToTime(d, times.lastOption, dayOfWeekTimes))
            )
        )
    }

    def analyseMultipleDatesPattern(dates: Vector[LocalDate],
                                    times: Vector[LocalTime]): Set[(LocalDateTime, Option[LocalDateTime])] = {

        if (dates.length != times.length) {
            val fromTime = times.headOption
            val toTime = times.lastOption

            dates.map(d => (dateWithFromTime(d, fromTime), Some(dateWithToTime(d, toTime)))).toSet
        } else {
            dates.zip(times)
                .map {
                    case (date: LocalDate, time: LocalTime) => (dateWithFromTime(date, Some(time)), None)
                }.toSet
        }
    }

    private def dateWithFromTime(date: LocalDate, time: Option[LocalTime], dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)] = Map()): LocalDateTime = {
        LocalDateTime.of(date,
            if (dayOfWeekTimes.isEmpty || dayOfWeekTimes.get(date.getDayOfWeek).isEmpty) time.getOrElse(LocalTime.of(0, 0))
            else {
                dayOfWeekTimes(date.getDayOfWeek)._1
            }
        )
    }

    private def dateWithToTime(date: LocalDate, time: Option[LocalTime], dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)] = Map()): LocalDateTime = {
        LocalDateTime.of(date,
            if (dayOfWeekTimes.isEmpty || dayOfWeekTimes.get(date.getDayOfWeek).isEmpty) time.getOrElse(LocalTime.of(0, 0))
            else {
                dayOfWeekTimes(date.getDayOfWeek)._2
            }
        )
    }
}
