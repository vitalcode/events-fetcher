package uk.vitalcode.events.fetcher.utils

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtil {
    def getColumnValue(result: Result, family: String, column: String): String = {
        Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
    }
}
