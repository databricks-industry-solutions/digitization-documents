package com.databricks.labs.tika

import org.apache.spark.sql.SparkSession
import org.apache.tika.io.TikaInputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream
import java.nio.file.Paths

class TikaExtractorTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Tika").master("local[1]").getOrCreate()

  "A tika extractor" should "extract content from files on disk" in {
    val path = Paths.get("src", "test", "resources", "text")
    val allFiles = path.toFile.listFiles()
    allFiles.length should be > 0
    allFiles.map(file => {
      val document = new TikaExtractor().extract(TikaInputStream.get(new FileInputStream(file)), file.toString)
      val text = document.content
      text should include regex "[hH]ello"
      text should include regex "[tT]ika"
    })
  }

  it should "be able to extract content from images" in {
    val path = Paths.get("src", "test", "resources", "images")
    val allFiles = path.toFile.listFiles()
    allFiles.length should be > 0
    allFiles.map(file => {
      val document = new TikaExtractor().extract(TikaInputStream.get(new FileInputStream(file)), file.toString)
      val text = document.content
      text should include regex "[hH]ello"
      text should include regex "[tT]ika"
    })
  }

  it should "implement spark reader" in {
    val path1 = Paths.get("src", "test", "resources", "text").toAbsolutePath.toString
    val path2 = Paths.get("src", "test", "resources", "images").toAbsolutePath.toString
    val df = spark.read.format("com.databricks.labs.tika.TikaFileFormat").option("foo", "bar").load(path1, path2)
    df.show()
    df.count() shouldBe 15
  }

  it should "work with registered short name" in {
    val path = Paths.get("src", "test", "resources", "text").toAbsolutePath.toString
    val df = spark.read.format("tika").option("foo", "bar").load(path)
    df.select("contentText").show()
  }

  "A Suspicious activity report" should "be read" in {
    val path = Paths.get("src", "test", "resources", "SAR.png")
    val document = new TikaExtractor().extract(TikaInputStream.get(new FileInputStream(path.toFile)), path.toFile.toString)
    val text = document.content
    assert(text.contains("John Doe"))
    println(text)
  }

}
