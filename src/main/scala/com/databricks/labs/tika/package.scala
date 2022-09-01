package com.databricks.labs

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

package object tika {

  private[tika] val schema: StructType = StructType(
    StructField("path", StringType, nullable = false) ::
      StructField("length", LongType, nullable = false) ::
      StructField("modificationTime", TimestampType, nullable = false) ::
      StructField("content", BinaryType, nullable = false) ::
      StructField("contentType", StringType, nullable = false) ::
      StructField("contentText", StringType, nullable = false) ::
      StructField("contentMetadata", MapType(StringType, StringType), nullable = false) ::
      Nil
  )

  private[tika] case class Extracted(
                                      content: String,
                                      metadata: Map[String, String]
                                    )

  private[tika] case class DocumentRow(
                                        fileName: String,
                                        fileTime: Long,
                                        fileLength: Long,
                                        binaryData: Array[Byte],
                                        extracted: Extracted
                                      ) {

    val mimeType: Option[String] = extracted.metadata.get("Content-Type")

    /**
     * UnsafeRow is an InternalRow for mutable binary rows that are backed by raw memory outside the JVM
     * (instead of Java objects that are in JVM memory space and may lead to more frequent GCs if created in excess)
     * @return UnsafeRow constructing our resulting dataset
     */
    def toRow: UnsafeRow = {

      val writer = new UnsafeRowWriter(schema.length)
      writer.resetRowWriter()

      // Let's write the atomic type first, serializing strings with UTF8String util
      writer.write(0, UTF8String.fromString(fileName))
      writer.write(1, fileLength)
      writer.write(2, fileTime)
      writer.write(3, binaryData)
      if (mimeType.isDefined) {
        writer.write(4, UTF8String.fromString(mimeType.get))
      }
      writer.write(5, UTF8String.fromString(extracted.content))

      // MapType is not considered mutable, we need to convert to UnsafeRow record
      // This will be a structure for list of keys, list of values
      val previousCursor = writer.cursor()
      val dataType = MapType(StringType, StringType)
      val keyArrayWriter = new UnsafeArrayWriter(writer, dataType.defaultSize)
      val valArrayWriter = new UnsafeArrayWriter(writer, dataType.defaultSize)

      // preserve 8 bytes to write the key array numBytes later.
      valArrayWriter.grow(8)
      valArrayWriter.increaseCursor(8)

      // Write the keys and write the numBytes of key array into the first 8 bytes.
      keyArrayWriter.initialize(extracted.metadata.size)
      extracted.metadata.keys.zipWithIndex.foreach({ case (k, i) =>
        keyArrayWriter.write(i, UTF8String.fromString(k))
      })

      // Shift cursor for array of values
      Platform.putLong(
        valArrayWriter.getBuffer,
        previousCursor,
        valArrayWriter.cursor - previousCursor - 8
      )

      // Write the values.
      valArrayWriter.initialize(extracted.metadata.size)
      extracted.metadata.values.zipWithIndex.foreach({ case (v, i) =>
        valArrayWriter.write(i, UTF8String.fromString(v))
      })

      // Write our MapType - phew...
      writer.setOffsetAndSizeFromPreviousCursor(6, previousCursor)
      writer.getRow

    }
  }
}
