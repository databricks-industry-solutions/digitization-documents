package com.databricks.labs.tika

import com.sun.jna.NativeLibrary
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ocr.TesseractOCRConfig
import org.apache.tika.parser.pdf.PDFParserConfig
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser}
import org.apache.tika.sax.BodyContentHandler
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, IOException}
import scala.xml.SAXException

class TikaExtractor extends Serializable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def isTesseractAvailable: Boolean = {
    NativeLibrary.getInstance("tesseract")
    try {
      NativeLibrary.getInstance("tesseract")
      logger.info("Found tesseract library")
      true
    } catch {
      case _: UnsatisfiedLinkError =>
        logger.warn("Could not find tesseract library")
        false
    }
  }

  @throws[IOException]
  @throws[SAXException]
  @throws[TikaException]
  val extract: Array[Byte] => Extracted = (stream: Array[Byte]) => {

    val parser = new AutoDetectParser()
    val handler = new BodyContentHandler()
    val metadata = new Metadata()

    val pdfConfig = new PDFParserConfig
    val parseContext = new ParseContext
    val config = new TesseractOCRConfig

    // To work, we need Tesseract library install natively
    // Input format will not fail, but won't be able to extract text from pictures
    // TODO: Try to find a way to log warning
    isTesseractAvailable

    parseContext.set(classOf[TesseractOCRConfig], config)
    parseContext.set(classOf[PDFParserConfig], pdfConfig)
    parseContext.set(classOf[Parser], parser)

    val is = new ByteArrayInputStream(stream)
    parser.parse(is, handler, metadata, parseContext)
    val content = handler.toString
    is.close()
    Extracted(content, metadata.names().map(name => (name, metadata.get(name))).toMap)
  }

}
