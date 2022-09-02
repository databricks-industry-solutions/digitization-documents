package com.databricks.labs.tika

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.apache.tika.io.TikaInputStream

import java.io.ByteArrayInputStream
import java.net.URI

class TikaFileFormat extends FileFormat with DataSourceRegister {

  // We do not infer the schema (such as CSV, JSON, etc. Our schema is fixed
  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = Some(schema)

  // This is an input format only, we do not create write capabilities
  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    throw QueryExecutionErrors.writeUnsupportedForBinaryFileDataSourceError()
  }

  // Files are read as binary and need to be read as a whole
  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    false
  }

  // We will enable our format to be used by its short name
  // spark.read.format("tika")
  // Assuming we defined our parser in src/main/resources/META-INF
  override def shortName(): String = "tika"

  override protected def buildReader(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val hadoopConf_B = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val maxLength = sparkSession.conf.get("spark.sql.sources.binaryFile.maxLength").toInt
    val tikaExtractor = new TikaExtractor()

    file: PartitionedFile => {

      // Retrieve file information
      val path = new Path(new URI(file.filePath))
      val fs = path.getFileSystem(hadoopConf_B.value.value)
      val status = fs.getFileStatus(path)
      if (status.getLen > maxLength) throw QueryExecutionErrors.fileLengthExceedsMaxLengthError(status, maxLength)
      val fileName = status.getPath.toString
      val fileLength = status.getLen
      val fileTime = DateTimeUtils.millisToMicros(status.getModificationTime)

      // Open file as a stream
      val inputStream = fs.open(status.getPath)

      try {

        // Fully read file content as ByteArray
        val byteArray = IOUtils.toByteArray(inputStream)

        // Extract text from binary using Tika
        val tikaDocument = tikaExtractor.extract(TikaInputStream.get(new ByteArrayInputStream(byteArray)), fileName)

        // Write content to a row following schema specs
        val writer = new UnsafeRowWriter(requiredSchema.length)
        writer.resetRowWriter()

        // Append each field to a row in the order dictated by our schema
        requiredSchema.zipWithIndex.foreach({ case (f, i) =>
          f.name match {
            case COL_PATH => writer.write(i, UTF8String.fromString(fileName))
            case COL_TIME => writer.write(i, fileTime)
            case COL_LENGTH => writer.write(i, fileLength)
            case COL_TEXT => writer.write(i, UTF8String.fromString(tikaDocument.content))
            case COL_CONTENT => writer.write(i, byteArray)
            case COL_TYPE => writer.write(i, UTF8String.fromString(tikaDocument.contentType))
            case COL_METADATA =>

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
              keyArrayWriter.initialize(tikaDocument.metadata.size)
              tikaDocument.metadata.keys.zipWithIndex.foreach({ case (k, i) =>
                keyArrayWriter.write(i, UTF8String.fromString(k))
              })

              // Shift cursor for array of values
              Platform.putLong(
                valArrayWriter.getBuffer,
                previousCursor,
                valArrayWriter.cursor - previousCursor - 8
              )

              // Write the values.
              valArrayWriter.initialize(tikaDocument.metadata.size)
              tikaDocument.metadata.values.zipWithIndex.foreach({ case (v, i) =>
                valArrayWriter.write(i, UTF8String.fromString(v))
              })

              // Write our MapType - phew...
              writer.setOffsetAndSizeFromPreviousCursor(i, previousCursor)

            case other => throw QueryExecutionErrors.unsupportedFieldNameError(other)
          }
        })

        // Return a row of TIKA extracted content
        Iterator.single(writer.getRow)

      } finally {
        IOUtils.close(inputStream)
      }
    }
  }
}
