package com.datenwissenschaften

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport

/**
 * Reads one [[MDFPartition]]'s sample values on the executor.
 *
 * When only the `channel` column was requested (after column pruning), rows are synthesized locally from the
 * partition's known cycle count without invoking Python at all. Otherwise this shells out to `python -m
 * mdf_spark.helper read` as a fresh subprocess per partition, because an executor in a real (non-local) cluster runs in
 * a separate JVM from the driver and cannot call back into the driver's [[MDFPythonBridge]] — see the memoized
 * `get_partitions` cache on the Python side for the driver-side equivalent, which this deliberately does not share. The
 * helper's output is a Parquet file path, read via [[readParquet]] and deleted once consumed.
 */
class MDFPartitionReader(partition: MDFPartition, readSchema: StructType) extends PartitionReader[InternalRow] {
  private var rows: Iterator[InternalRow] = Iterator.empty
  private var initialized = false
  private var parquetPath: String = null

  private def init(): Unit = {
    if (!initialized) {
      val parts = partition.data.split("\\|")
      if (parts.length == 5) {
        val channel = parts(0)
        val group = parts(1)
        val index = parts(2)
        val offset = parts(3)
        val count = parts(4).toLong
        val requestedFields = readSchema.fieldNames
        if (requestedFields.length == 1 && requestedFields(0) == "channel" && count > 0) {
          rows = (0L until count).map(_ => InternalRow(UTF8String.fromString(channel))).iterator
        } else {
          // Reading real channel values runs on the executor, which in a real
          // (non-local) cluster is a separate JVM process from the driver
          // that owns the py4j bridge — so it must invoke Python as its own
          // subprocess rather than calling back into the driver's bridge.
          // --group/--index identify the exact occurrence the driver already
          // resolved during partition planning, so the helper reads that one
          // occurrence directly instead of re-resolving it via whereis().
          // --offset/--count identify this partition's own record range
          // within that occurrence (see MDFPythonBridge / get_partitions):
          // asammdf's record_offset/record_count support lets it decode only
          // that range, which is what makes a single large channel readable
          // as more than one partition in the first place.
          val pythonExec = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
          val cmd = Seq(
            pythonExec,
            "-m",
            "mdf_spark.helper",
            "read",
            "--path",
            partition.filePath,
            "--channel",
            channel,
            "--group",
            group,
            "--index",
            index,
            "--offset",
            offset,
            "--count",
            count.toString,
            "--where",
            partition.whereClause
          )

          val outputLines = new ArrayBuffer[String]
          val stderr = new StringBuilder
          val logger = ProcessLogger(s => outputLines += s, s => stderr.append(s).append("\n"))
          val exitCode = Process(cmd, None, sys.env.toSeq: _*).!(logger)

          if (exitCode != 0) {
            throw new RuntimeException(
              s"Python helper failed with exit code $exitCode. \nSTDOUT: ${outputLines.mkString("\n")}\nSTDERR: ${stderr.toString()}"
            )
          }

          val parquetLine = outputLines.find(_.startsWith("PARQUET:"))
          if (parquetLine.isDefined) {
            parquetPath = parquetLine.get.substring(8)
            rows = readParquet(parquetPath)
          }
        }
      }
      initialized = true
    }
  }

  private def readParquet(path: String): Iterator[InternalRow] = {
    val conf = new Configuration()
    val reader = ParquetReader.builder(new GroupReadSupport(), new Path(path)).withConf(conf).build()

    new Iterator[InternalRow] {
      private var nextRow: InternalRow = null
      private var finished = false

      override def hasNext: Boolean = {
        if (finished) return false
        if (nextRow != null) return true

        val group = reader.read()
        if (group == null) {
          finished = true
          reader.close()
          new File(path).delete() // Cleanup
          return false
        }

        val rowValues = readSchema.fields.map { field =>
          field.name match {
            case "time"    => group.getLong("time", 0)
            case "channel" => UTF8String.fromString(group.getString("channel", 0))
            case "valueNumeric" =>
              if (group.getType.containsField("valueNumeric") && group.getFieldRepetitionCount("valueNumeric") > 0) {
                group.getDouble("valueNumeric", 0)
              } else {
                // Genuinely null (a discrete/string-valued channel's row), not 0.0 — matching
                // valueText's null handling below, so exactly one of the two columns is
                // non-null per row, as documented on MDFDataSource.
                null
              }
            case "valueText" =>
              if (group.getType.containsField("valueText") && group.getFieldRepetitionCount("valueText") > 0) {
                UTF8String.fromString(group.getString("valueText", 0))
              } else {
                null
              }
            case _ => null
          }
        }

        nextRow = InternalRow.fromSeq(rowValues.toIndexedSeq)
        true
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException()
        val res = nextRow
        nextRow = null
        res
      }
    }
  }

  override def next(): Boolean = {
    init()
    rows.hasNext
  }

  override def get(): InternalRow = {
    rows.next()
  }

  override def close(): Unit = {
    if (parquetPath != null) {
      val file = new File(parquetPath)
      if (file.exists()) {
        file.delete()
      }
    }
  }
}
