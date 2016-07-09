package uk.vitalcode.events.fetcher.parser.dateParser

import java.time._
import java.time.format.DateTimeFormatter

import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.parser.ParserLike
import uk.vitalcode.events.model.Prop

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

        def logInfo(text: String): Unit = log.info(s"ParseAsDateTime: $text]")

        logInfo(s"parsing prop: [$prop]")
        logInfo(s"parsing prop value: [${prop.values.mkString(" ")}]")

        val tokens = splitRegEx.split(prop.values.mkString(" ")).filter(t => !t.isEmpty)
            .flatMap(t => {
                DateTokenFactory.create(t.trim())
            })

        val dates: Vector[LocalDate] = (1 to 4)
            .flatMap(i => tokens.grouped(i)
                .flatMap(g => {
                    val year = g.find(ty => ty.isInstanceOf[YearToken]).map(ty => ty.asInstanceOf[YearToken])
                    val month = g.find(tm => tm.isInstanceOf[MonthToken]).map(ty => ty.asInstanceOf[MonthToken])
                    val dayOfMonth = g.find(ty => ty.isInstanceOf[DayOfMonthToken]).map(ty => ty.asInstanceOf[DayOfMonthToken])
                    if (month.nonEmpty && dayOfMonth.nonEmpty) {
                        Vector(DateToken(LocalDate.of(
                            if (year.isEmpty) LocalDateTime.now().getYear else year.get.value,
                            month.get.value,
                            dayOfMonth.get.value)))
                    } else g
                })
                .filter(d => d.isInstanceOf[DateToken])
                .map(d => d.asInstanceOf[DateToken].value)
            ).distinct.toVector

        val times: Vector[LocalTime] = tokens.flatMap(t => t match {
            case t: TimeToken => Vector(t.value)
            case _ => None
        }).toVector

        val dayOfWeekTimes: Map[DayOfWeek, (LocalTime, LocalTime)] = (0 to 2)
            .flatMap(i => tokens.drop(i).grouped(3)
                .flatMap {
                    case Array(w: DayOfWeekToken, t1: TimeToken, t2: TimeToken) => Map(w.value ->(t1.value, t2.value))
                    case _ => Nil
                }
            ).toMap

        val daysOfWeek: Set[DayOfWeek] = tokens.flatMap {
            case t: DayOfWeekToken => Set(t.value)
            case _ => Nil
        }.toSet


        logInfo(s"tokens [${tokens.length}] [${tokens.mkString(",")}]")
        logInfo(s"dates [${dates.size}] [$dates]")
        logInfo(s"times [${times.size}] [$times]")
        logInfo(s"dayOfWeekTimes [${dayOfWeekTimes.size}] [$dayOfWeekTimes]")
        logInfo(s"daysOfWeek [${daysOfWeek.size}] [$daysOfWeek]")

        PatternAnalyser(dates, times, daysOfWeek, dayOfWeekTimes)
    }
}

