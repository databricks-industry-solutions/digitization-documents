package com.databricks.labs.tika

import org.apache.tika.exception.TikaException
import org.apache.tika.io.TikaInputStream
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
    parseContext.set(classOf[TesseractOCRConfig], config)
    parseContext.set(classOf[PDFParserConfig], pdfConfig)
    parseContext.set(classOf[Parser], parser)

    // Need to use Tika InputStream to read enough bytes and auto detect parser
    // Otherwise we may have inconsistent results (e.g. docx considered as zip file)
    val is = TikaInputStream.get(new ByteArrayInputStream(stream))
    parser.parse(is, handler, metadata, parseContext)
    val content = handler.toString
    is.close()
    Extracted(content, metadata.names().map(name => (name, metadata.get(name))).toMap)
  }

}
