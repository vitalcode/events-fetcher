package uk.vitalcode.events.fetcher.parser.dateParser

import java.text.DateFormatSymbols
import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField._
import java.util.Locale

import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.parser.ParserLike
import uk.vitalcode.events.fetcher.utils.DateTimeUtil
import uk.vitalcode.events.model.Prop

import scala.util.Try

object DateParser extends ParserLike[(String, Option[String])] with Log {

    // Split on white space or "-" (range character, including "-" as return token, but not before AM/PM)
    val splitRegEx =
        """(?<![-])[\s]+(?![-]|PM|pm|AM|am)|(?=[-,])|(?<=[-,])""".r

    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    override def parse(prop: Prop): Vector[(String, Option[String])] = {
        parseAsDateTime(prop).map(d => {
            val to: Option[String] = if (d._2.isDefined) Some(d._2.get.format(dateFormatter)) else None
            Tuple2(d._1.format(dateFormatter), to)
        }).toVector
    }

    def parseAsDateTime(prop: Prop): Set[(LocalDateTime, Option[LocalDateTime])] = {

        log.info(s"ParseAsDateTime: props: [${prop}]")
        log.info(s"ParseAsDateTime: props value: [${prop.values.mkString(" ")}]")

        val tokens = splitRegEx.split(prop.values.mkString(" ")).filter(t => !t.isEmpty)
            .flatMap(t => {
                DateTokenFactory.create(t.trim())
            })

        log.info(s"ParseAsDateTime: date tokens [${tokens.size}] [${tokens.mkString(",")}]")

        val dates: Vector[LocalDate] = (1 to 4)
            .flatMap(i => tokens.grouped(i)
                .flatMap(g => {
                    val year = g.find(ty => ty.isInstanceOf[YearToken]).map(ty => ty.asInstanceOf[YearToken])
                    val month = g.find(tm => tm.isInstanceOf[MonthToken]).map(ty => ty.asInstanceOf[MonthToken])
                    val dayOfMonth = g.find(ty => ty.isInstanceOf[DayOfMonthToken]).map(ty => ty.asInstanceOf[DayOfMonthToken])
                    if (month.nonEmpty && dayOfMonth.nonEmpty) {
                        Vector(DateToken(LocalDate.of(
                            if (year.isEmpty) LocalDateTime.now().getYear else year.get.token.toInt,
                            month.get.getMonth,
                            dayOfMonth.get.token.toInt)))
                    } else g
                })
                .filter(d => d.isInstanceOf[DateToken])
                .map(d => d.asInstanceOf[DateToken].dateTime)
            ).distinct.toVector

        log.info(s"ParseAsDateTime: date dates [${dates.size}] [${dates}]")

        val times: Vector[LocalTime] = tokens.flatMap(t => t match {
            case t: TimeToken => Vector(t.time)
            case _ => None
        }).toVector

        log.info(s"ParseAsDateTime: date times [${times.size}] [${times}]")

        val dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)] = (0 to 2)
            .flatMap(i => tokens.drop(i).grouped(3)
                .flatMap {
                    case Array(w: DayOfWeekToken, t1: TimeToken, t2: TimeToken) => Map(w.token ->(t1.time, t2.time))
                    case _ => Nil
                }
            ).toMap

        log.info(s"ParseAsDateTime: date dayOfWeekTimes [${dayOfWeekTimes.size}] [${dayOfWeekTimes}]")

        val daysOfWeek: Set[DayOfWeek] = tokens.flatMap {
            case t: DayOfWeekToken => Set(t.token)
            case _ => Nil
        }.toSet

        log.info(s"ParseAsDateTime: date daysOfWeek [${daysOfWeek.size}] [${daysOfWeek}]")

        PatternAnalyser(dates, times, daysOfWeek, dayOfWeekTimes)
    }
}


object DateTokenFactory {

    // Matches times seperated by either : or . will match a 24 hour time, or a 12 hour time with AM or PM specified. Allows 0-59 minutes, and 0-59 seconds. Seconds are not required.
    // Replace (AM|am|aM|Am|PM|pm|pM|Pm) with (?i:(a|p)(\.)?( )?m(\.)?) and you will match any combination of am/pm (case insensitive) including with periods and spaces (i.e., AM, A.M., or A. M.)
    // Matches: 1:01 AM | 23:52:01 | 03.24.36 AM
    // Non-Matches: 19:31 AM | 9:9 PM | 25:60:61
    // From: http://regexlib.com/REDetails.aspx?regexp_id=144
    private val timeRegEx =
        """((([0]?[1-9]|1[0-2])((:|\.)[0-5][0-9])?((:|\.)[0-5][0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?))""".r
    //"""^((([0]?[1-9]|1[0-2])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?))$""".r

    // Matches year in interval 1900-2099
    // From: http://stackoverflow.com/questions/4374185/regular-expression-match-to-test-for-a-valid-year
    private val yearRegEx =
        """(19|20)\d{2}""".r

    private val monthRegEx = """(?i)january|february|march|april|may|june|july|august|september|october|november|december""".r

    private val dayOfMonthRegEx = """[0123]?[0-9]""".r //TODO fix

    //private val dayOfWeekRegEx = """^(Mo(n(day)?)?|Tu(e(sday)?)?|We(d(nesday)?)?|Th(u(rsday)?)?|Fr(i(day)?)?|Sa(t(urday)?)?|Su(n(day)?)?)$""".r

    def create(token: String): Option[DateTokenLike] = {
        var week = new DateFormatSymbols(Locale.UK).getWeekdays.map(w => w.toLowerCase()).indexOf(token.toLowerCase())
        week = if (week != -1) week else new DateFormatSymbols(Locale.UK).getShortWeekdays.map(w => w.toLowerCase()).indexOf(token.toLowerCase())
        if (week != -1) Some(DayOfWeekToken(DayOfWeek.of(if (week > 1) week - 1 else 7)))
        else {
            val time = timeRegEx.findFirstIn(token)
            if (time.nonEmpty) Some(TimeToken(time.get.replaceAll("\\s", "").toUpperCase()))
            else {
                val year = yearRegEx.findFirstIn(token)
                if (year.nonEmpty) Some(YearToken(year.get))
                else {
                    var month = new DateFormatSymbols(Locale.UK).getMonths.map(m => m.toLowerCase()).indexOf(token.toLowerCase()) //monthRegEx.findFirstIn(token)
                    month = if (month != -1) month else new DateFormatSymbols(Locale.UK).getShortMonths.map(m => m.toLowerCase()).indexOf(token.toLowerCase())
                    if (month != -1) Some(MonthToken((month + 1).toString))
                    else {
                        val dayOfMonth = dayOfMonthRegEx.findFirstIn(token)
                        if (dayOfMonth.nonEmpty) Some(DayOfMonthToken(dayOfMonth.get))
                        else None
                    }
                }
            }
        }
    }
}
