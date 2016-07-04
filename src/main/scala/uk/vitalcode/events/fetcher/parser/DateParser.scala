package uk.vitalcode.events.fetcher.parser

import java.text.DateFormatSymbols
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField._
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.Locale

import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.service.PropertyService._
import uk.vitalcode.events.fetcher.utils.DateTimeUtil
import uk.vitalcode.events.model.Prop

import scala.util.Try

object DateParser extends ParserLike[(String, String)] with Log {

    // Split on white space or "-" (range character, including "-" as return token, but not before AM/PM)
    val splitRegEx =
        """(?<![-])[\s]+(?![-]|PM|pm|AM|am)|(?=[-,])|(?<=[-,])""".r

    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    override def parse(prop: Prop): Set[(String, String)] = {
        parseAsDateTime(prop).map(d => Tuple2(d._1.format(dateFormatter), d._2.format(dateFormatter)))
    }

    def parseAsDateTime(prop: Prop): Set[(LocalDateTime, LocalDateTime)] = {

        log.info(s"ParseAsDateTime: props: [${prop}]")
        log.info(s"ParseAsDateTime: props value: [${prop.values.mkString(" ")}]")

        val tokens = splitRegEx.split(prop.values.mkString(" ")).filter(t => !t.isEmpty)
            .flatMap(t => {
                DateTokenFactory.create(t.trim())
            })

        log.info(s"ParseAsDateTime: date tokens [${tokens.size}] [${tokens.mkString(",")}]")

        val dates: Vector[LocalDate] = tokens.grouped(3) // todo scan for dates 1 -> 5
            .flatMap(g => {
            val year = g.find(ty => ty.isInstanceOf[YearToken])
            val month = g.filter(tm => tm.isInstanceOf[MonthToken]).map(ty => ty.asInstanceOf[MonthToken]).headOption
            val dayOfMonth = g.find(ty => ty.isInstanceOf[DayOfMonthToken])
            if (month.nonEmpty && dayOfMonth.nonEmpty) {
                Vector(DateToken(LocalDate.of(
                    if (year.isEmpty) LocalDateTime.now().getYear else year.get.token.toInt,
                    month.get.getMonth,
                    dayOfMonth.get.token.toInt)))
            } else g
        })
            .filter(d => d.isInstanceOf[DateToken])
            .map(d => d.asInstanceOf[DateToken].dateTime)
            .toVector

        log.info(s"ParseAsDateTime: date dates [${dates.size}] [${dates}]")

        val times: Vector[LocalTime] = tokens.flatMap(t => t match {
            case t: TimeToken => Vector(t.time)
            case _ => None
        }).toVector

        log.info(s"ParseAsDateTime: date times [${times.size}] [${times}]")

        dates.size match {
            case 1 => analyseOneDatePattern(dates, times)
            case 2 => analyseDateRangePattern(dates, times)
            case _ => {
                ???
            }
        }

        // analyseOneDatePattern(dates, times)
    }

    // TODO make as strategy
    private def analyseOneDatePattern(dates: Vector[LocalDate], times: Vector[LocalTime]): Set[(LocalDateTime, LocalDateTime)] = {
        val fromDate = dateWithTime(dates.head, times.headOption)
        val toDate = dateWithTime(dates.head, times.lastOption)
        Set((fromDate, toDate))
        //        Set((fromDate, toDate), (fromDate.plusDays(2), toDate.plusDays(2)))
    }

    // TODO need to pass dates as LocalDate
    private def analyseDateRangePattern(dates: Vector[LocalDate], times: Vector[LocalTime]): Set[(LocalDateTime, LocalDateTime)] = {
        DateTimeUtil.datesInRange(dates(0), dates(1))
            .map(d => (dateWithTime(d, times.headOption), dateWithTime(d, times.lastOption)))
    }

    private def dateWithTime(date: LocalDate, time: Option[LocalTime]): LocalDateTime = {
        LocalDateTime.of(date, time.getOrElse(LocalTime.of(0, 0)))
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

    def create(token: String): Option[DateTokenLike] = {
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


trait DateTokenLike {
    def token: String
}

case class YearToken(token: String) extends DateTokenLike

case class MonthToken(token: String) extends DateTokenLike {
    def getMonth: Int = {
        token.toInt
    }
}

case class DayOfMonthToken(token: String) extends DateTokenLike

case class DateToken(dateTime: LocalDate) extends DateTokenLike {
    def token: String = {
        "ddd"
    }
}

case class TimeToken(token: String) extends DateTokenLike {


    private val formatter24H = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .toFormatter

    private val formatter12H = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .appendText(AMPM_OF_DAY)
        .toFormatter()

    private val formatter12HwithDot = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral('.')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral('.')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .appendText(AMPM_OF_DAY)
        .toFormatter()

    //    private val formatter12HhoursOnly = new DateTimeFormatterBuilder()
    //        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
    //        .appendLiteral(':')
    //        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
    //        .optionalStart()
    //        .appendLiteral(':')
    //        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
    //        .optionalStart()
    //        .appendFraction(NANO_OF_SECOND, 0, 9, true)
    //        .optionalEnd()
    //        .optionalEnd()
    //        .appendText(AMPM_OF_DAY)
    //        .toFormatter()

    //    private def format24H: Option[LocalTime] = {
    //        try {
    //            Some(LocalTime.parse(token, formatter24H))
    //        } catch {
    //            case _: Throwable => None
    //        }
    //    }

    private def format24H: Option[LocalTime] = Try {
        LocalTime.parse(token, formatter24H)
    }.toOption

    private def format12H: Option[LocalTime] = Try {
        if (token.contains(':')) LocalTime.parse(token, formatter12H)
        else LocalTime.parse(token, formatter12HwithDot)
    }.toOption

    //        catch {
    //            case _: Throwable => None
    //            //Some(LocalTime.parse("11:11pm", formatter12H)) // FIX if not valid time e.g. Some(LocalTime.parse("11:11pm", formatter12H)) // FIX if not valid time e.g.
    //        }
    //  }

    def time: LocalTime = {
        if (format12H.isEmpty) {
            if (format24H.isEmpty) LocalTime.parse("11:11PM", formatter12H) ///  // refactor may return null
            else format24H.get
        }
        else format12H.get
    }
}