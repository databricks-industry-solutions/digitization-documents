package com.databricks.labs.tika

import org.apache.tika.exception.TikaException
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaCoreProperties}
import org.apache.tika.parser.microsoft.OfficeParserConfig
import org.apache.tika.parser.ocr.TesseractOCRConfig
import org.apache.tika.parser.pdf.PDFParserConfig
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser}
import org.apache.tika.sax.BodyContentHandler
import org.slf4j.LoggerFactory

import java.io.IOException
import scala.xml.SAXException

class TikaExtractor extends Serializable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[IOException]
  @throws[SAXException]
  @throws[TikaException]
  def extract(stream: TikaInputStream, filename: String): TikaRecord = {

    // Configure each parser if required
    val pdfConfig = new PDFParserConfig
    val officeConfig = new OfficeParserConfig
    val tesseractConfig = new TesseractOCRConfig
    val parseContext = new ParseContext

    val parser = new AutoDetectParser()
    val handler = new BodyContentHandler()
    val metadata = new Metadata()

    // To work, we need Tesseract library install natively
    // Input format will not fail, but won't be able to extract text from pictures
    parseContext.set(classOf[TesseractOCRConfig], tesseractConfig)
    parseContext.set(classOf[PDFParserConfig], pdfConfig)
    parseContext.set(classOf[OfficeParserConfig], officeConfig)
    parseContext.set(classOf[Parser], parser)

    // Tika will try at best effort to detect MIME-TYPE by reading some bytes in. With DOCx object, Tika is fooled
    // thinking it is just another zip file. It works better when passing a file than a stream as file name is also
    // leverage. So let's fool the fool by explicitly passing filename
    metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, filename)
    val contentType = parser.getDetector.detect(stream, metadata)

    // Extract content using the appropriate parsing
    parser.parse(stream, handler, metadata, parseContext)
    val content = handler.toString
    stream.close()

    // Return extracted content
    TikaRecord(
      content,
      contentType.toString,
      metadata.names().map(name => (name, metadata.get(name))).toMap
    )

  }

}
