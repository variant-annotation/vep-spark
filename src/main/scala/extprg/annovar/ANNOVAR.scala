package extprg.annovar

import org.apache.spark.{SparkContext, TaskContext}
import utils.ArgumentOption
import utils.CustomOperators._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.control.Breaks.{break, breakable}

object ANNOVAR {
  def getSuffixList(prjTmpDir: File): ArrayBuffer[String] = {
    println("Suffix List:")
    val files = prjTmpDir.listFiles()
    var suffix = new String
    val sufList = new ArrayBuffer[String]
    for (file <- files) {
      suffix = file.getName.split("[.]", 2)(1)
      if (!sufList.contains(suffix)) sufList += suffix
    }
    sufList
  }

  def firstLine(f: File): Option[String] = {
    val src = Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }

  def generateVcfHeaderAnnotationINFO(prjTmpDir: File): ArrayBuffer[String] = {
    val files = prjTmpDir.listFiles()

    val annotateInfos = new ArrayBuffer[String]

    breakable(
      for (file <- files) if (file.getName.endsWith(".txt")) {
        firstLine(file).get.split("\t").map(col => {
          if (
            !col.contains("Otherinfo") &&
              !col.equals("Chr") &&
              !col.equals("Start") &&
              !col.equals("End") &&
              !col.equals("Ref") &&
              !col.equals("Alt")
          ) annotateInfos += "##INFO=<ID=" + col + ",Number=.,Type=String,Description=\"" + col + " annotation provided by ANNOVAR\">"
        })
        break
      }
    )
    if (annotateInfos.nonEmpty) {
      val openTag = "##INFO=<ID=ANNOVAR_DATE,Number=1,Type=String,Description=\"Flag the start of ANNOVAR annotation for one alternative allele\">"
      val closeTag = "##INFO=<ID=ALLELE_END,Number=0,Type=Flag,Description=\"Flag the end of ANNOVAR annotation for one alternative allele\">"
      openTag +=: annotateInfos += closeTag
    }
    annotateInfos
  }


  def annotateByAnnovar(sc: SparkContext, args: Array[ArgumentOption], execDir: String): Unit = {


    val vcfRDD = sc.textFile("/home/ethan/annovar/example/ex2.vcf")
    //    val vcfRDD = sc.textFile("/home/ethan/vinbdi/ALL.chrY.phase3_integrated_v1a.20130502.genotypes.vcf")

    val prjPath = getClass.getResource("").getPath
    val tmpDirName = "vaspark_tmp"
    val prjTmpDir = new File(prjPath, tmpDirName)
    if (prjTmpDir.exists())
      if (prjTmpDir.isDirectory) new Directory(prjTmpDir).deleteRecursively()
      else prjTmpDir.delete()
    prjTmpDir.mkdir()

    val annovarPath = "/home/ethan/annovar/"
    val outputPath = "/home/ethan/annovar/output/hehehe"
    val annotateCommand = "table_annovar.pl /dev/stdin /home/ethan/annovar/humandb/ -buildver hg19 -remove -protocol refGene,cytoBand,dbnsfp30a -operation g,r,f -nastring . -vcfinput -out "
    val (inputHeaderRDD, variantsRDD) = vcfRDD.filterDivisor(line => line.startsWith("#"))

    val numOfPartitions = variantsRDD.getNumPartitions
    variantsRDD.mapPartitions(partition => {
      val pId = TaskContext.get().partitionId()
      val annovarCommand = annovarPath + annotateCommand + prjTmpDir.getAbsolutePath + "/" + pId
      partition.pipeCmd(annovarCommand)
    }).collect()

    val suffixList = getSuffixList(prjTmpDir)

    var vcfHeader = ArrayBuffer(inputHeaderRDD.collect(): _*)
    vcfHeader = vcfHeader.patch(vcfHeader.length - 1, generateVcfHeaderAnnotationINFO(prjTmpDir), 0)
    vcfHeader.foreach(println)
    val annotatedHeaderRDD = sc.parallelize(vcfHeader)


    for (suffix <- suffixList) {
      val filePaths = new ArrayBuffer[String]
      if (suffix.endsWith(".txt")) {
        val txtHeaderRDD = sc.textFile(prjTmpDir + "/0." + suffix)
        for (i <- 1 until numOfPartitions) {
          val partFile = new File(prjTmpDir + "/" + i.toString + "." + suffix)
          if (partFile.exists()) filePaths += partFile.getAbsolutePath
        }
        val txtFilesRDD = sc.textFile(filePaths.mkString(",")).mapPartitionsWithIndex(
          (index, iterator) => if (index == 0) iterator.drop(1) else iterator
        )
        txtHeaderRDD.union(txtFilesRDD).saveAsSingleTextFile(outputPath + "." + suffix)
      } else {
        for (i <- 0 until numOfPartitions) {
          val partFile = new File(prjTmpDir + "/" + i.toString + "." + suffix)
          if (partFile.exists()) filePaths += partFile.getAbsolutePath
        }
        val outputRDD = sc.textFile(filePaths.mkString(","))
        if (suffix.endsWith(".vcf"))
          annotatedHeaderRDD.union(outputRDD).saveAsSingleTextFile(outputPath + "." + suffix)
        else
          outputRDD.saveAsSingleTextFile(outputPath + "." + suffix)
      }
    }
  }
}
