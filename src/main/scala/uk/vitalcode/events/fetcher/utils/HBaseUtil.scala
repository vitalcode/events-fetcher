package uk.vitalcode.events.fetcher.utils

import java.io.{ObjectInput, ObjectInputStream, _}

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtil {
    def getValueString(result: Result, family: String, column: String): String = {
        Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
    }

    def getValueObject[T](result: Result, family: String, column: String): T = {
        bytesToObject(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))).asInstanceOf[T]
    }

    def bytesToObject[T](bytes: Array[Byte]): T = {
        val bis: ByteArrayInputStream = new ByteArrayInputStream(bytes)
        var in: ObjectInput = null
        try {
            in = new ObjectInputStream(bis)
            in.readObject().asInstanceOf[T]
        } finally {
            try {
                bis.close()
            } catch {
                case ex: IOException =>
            }
            try {
                if (in != null) {
                    in.close()
                }
            } catch {
                case ex: IOException =>
            }
        }
    }

    def objectToBytes(obj: Any): Array[Byte] = {
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
        var out: ObjectOutput = null
        try {
            out = new ObjectOutputStream(bos)
            out.writeObject(obj)
            bos.toByteArray
        } finally {
            try {
                if (out != null) {
                    out.close()
                }
            } catch {
                case ex: IOException =>
            }
            try {
                bos.close()
            } catch {
                case ex: IOException =>
            }
        }
    }
}
