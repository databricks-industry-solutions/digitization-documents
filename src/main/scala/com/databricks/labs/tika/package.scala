package com.databricks.labs

import org.apache.spark.sql.types._

package object tika {

  private[tika] val COL_PATH = "path"
  private[tika] val COL_TIME = "modificationTime"
  private[tika] val COL_LENGTH = "length"
  private[tika] val COL_CONTENT = "content"
  private[tika] val COL_TYPE = "contentType"
  private[tika] val COL_TEXT = "contentText"
  private[tika] val COL_METADATA = "contentMetadata"

  private[tika] val schema: StructType = StructType(
    StructField(COL_PATH, StringType, nullable = false) ::
      StructField(COL_TIME, TimestampType, nullable = false) ::
      StructField(COL_LENGTH, LongType, nullable = false) ::
      StructField(COL_CONTENT, BinaryType, nullable = false) ::
      StructField(COL_TYPE, StringType, nullable = false) ::
      StructField(COL_TEXT, StringType, nullable = false) ::
      StructField(COL_METADATA, MapType(StringType, StringType), nullable = false) ::
      Nil
  )

  private[tika] case class TikaRecord(
                                       content: String,
                                       contentType: String,
                                       metadata: Map[String, String]
                                     )

}
