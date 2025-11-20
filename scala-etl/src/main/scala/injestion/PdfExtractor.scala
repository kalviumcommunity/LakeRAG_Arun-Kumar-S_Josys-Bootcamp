package ingestion

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

object PdfExtractor {

  def extract(spark: SparkSession, pathGlob: String): DataFrame = {
    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.globStatus(new Path(pathGlob)).map(_.getPath.toString)

    val rows = files.map { f =>
      val p = new Path(f)
      val stream = fs.open(p)
      val pdf = PDDocument.load(stream)
      val text = new PDFTextStripper().getText(pdf)
      pdf.close()
      stream.close()
      (f, text)
    }

    spark.createDataset(rows).toDF("file_path", "content")
  }
}
