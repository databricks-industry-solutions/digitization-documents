package com.databricks.labs.tika

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

class TikaFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = Some(schema)

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    throw QueryExecutionErrors.writeUnsupportedForBinaryFileDataSourceError()
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    false
  }

  override def shortName(): String = "tika"

  override protected def buildReader(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val hadoopConfB = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val maxLength = sparkSession.conf.get("spark.sql.sources.binaryFile.maxLength").toInt
    val tikaExtractor = new TikaExtractor()

    file: PartitionedFile => {
      val path = new Path(new URI(file.filePath))
      val fs = path.getFileSystem(hadoopConfB.value.value)
      val status = fs.getFileStatus(path)
      if (status.getLen > maxLength) throw QueryExecutionErrors.fileLengthExceedsMaxLengthError(status, maxLength)
      val stream = fs.open(status.getPath)
      try {
        val fileName = status.getPath.toString
        val fileLength = status.getLen
        val fileTime = DateTimeUtils.millisToMicros(status.getModificationTime)
        val binaryContent = IOUtils.toByteArray(stream)
        val tikaDocument = tikaExtractor.extract(binaryContent)
        val record = DocumentRow(fileName, fileTime, fileLength, binaryContent, tikaDocument)
        Iterator.single(record.toRow)
      } finally {
        IOUtils.close(stream)
      }
    }
  }
}
