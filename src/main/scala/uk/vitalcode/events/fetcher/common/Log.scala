package uk.vitalcode.events.fetcher.common

import org.apache.log4j.Logger

trait Log {
    val log = Logger.getLogger(this.getClass)
}
