package uk.vitalcode.events.fetcher.service

import uk.vitalcode.events.fetcher.common.StringUtils._

object EsQueryService {

    def matchAll(clue: String): String = {
        s"""{
            | "query": {
            |     "match": {
            |         "_all": {
            |             "query": "%s",
            |             "operator": "and"
            |         }
            |     }
            | }
            |}""".stripForJson.format(clue)
    }
}
