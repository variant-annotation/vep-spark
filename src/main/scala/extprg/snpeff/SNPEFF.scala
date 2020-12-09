package extprg.snpeff

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import utils.CustomOperators.RDDOperators

object SNPEFF {
  def annotateBySnpEff(
                     sc: SparkContext,
                     inputPath: String,
                     outputPath: String,
                     snpEffArgs: String,
                     snpEffJarPath: String) {
    val annotateCmd = "java -Xmx8g -jar " + snpEffJarPath + " " + snpEffArgs
    val vcfRDD = sc.textFile(inputPath)
    val (headerRDD, variantsRDD) = vcfRDD.filterDivisor(line => line.startsWith("#"))
    headerRDD
      .coalesce(1)
      .pipe(annotateCmd)
      .union(variantsRDD.pipe(annotateCmd).filter(line => !line.startsWith("#")))
      .saveAsSingleTextFile(outputPath)
  }
}
