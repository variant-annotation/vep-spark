package extprg.vep
import utils.{ArgumentOption, CommandBuilder, Tool}
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import utils.OutputHandler._
import org.apache.spark.rdd.RDD

object VEP {
  val usage = """
    You must input all parameters in usage to run this application
    Usage: vep-spark  [--input_file hdfs_absolute_dir]
                      [--output_file hdfs_absolute_dir]
                      [--vep_dir absolute_dir]
                      [--cache_dir absolute_dir]

    Optional arguments:
      All ensembl-vep arguments except "--cache", "--cache_dir", "--input_file", "--output_file"
      NOTE: If you use any ensembl-vep's plugin, please add [--dir_plugins absolute_dir] option
  """
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
       .appName("VEPSpark Application")
       .master("yarn")
       .getOrCreate()
    
    // List of ensemnl-vep's options
    val options: mutable.MutableList[ArgumentOption] = new mutable.MutableList[ArgumentOption]
    val arglist = args.toList

    type OptionMap = Map[String, Any]
    @scala.annotation.tailrec
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isValue(s : String) = (s(0) != '-')
      list match {
        case Nil => map
        case "--input_file" :: value :: tail =>
          nextOption(map ++ Map("input_file" -> value), tail)
        case "--output_file" :: value :: tail =>
          nextOption(map ++ Map("output_file" -> value), tail)
        case "--vep_dir" :: value :: tail =>
          nextOption(map ++ Map("vep_dir" -> value), tail)
        case "--cache_dir" :: value :: tail =>
          nextOption(map ++ Map("cache_dir" -> value), tail)
        case currentParam :: nextParam :: tail if isValue(nextParam) =>
          options += new ArgumentOption(currentParam, nextParam, true)
          nextOption(map, tail)
        case currentParam :: nextParam :: tail if !isValue(nextParam) =>
          options += new ArgumentOption(currentParam, null, false)
          nextOption(map, list.tail)
        case param :: Nil =>
          options += new ArgumentOption(param, null, false)
          nextOption(map, list.tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val defaultOptions = nextOption(Map(),arglist)
    if (
        !defaultOptions.contains("input_file") ||
        !defaultOptions.contains("output_file") ||
        !defaultOptions.contains("vep_dir") ||
        !defaultOptions.contains("cache_dir")
    ) {
      print(usage)
    } else {
      val builder = new CommandBuilder(Tool.VEP, defaultOptions.get("vep_dir").mkString, options)
      val cmd = builder.addOption("--cache", null, hasParameter = false)
        .addOption("--dir_cache", defaultOptions.get("cache_dir").mkString, hasParameter = true)
        .addOption("-o", "STDOUT", hasParameter = true)
        .build()
        .generate
      val input_path = defaultOptions.get("input_file").mkString
      val output_path = defaultOptions.get("output_file").mkString
      val number_of_partitions = 500
      val dataRDD = spark.sparkContext.textFile(input_path, minPartitions = number_of_partitions)
      val (headerRDD, variantsRDD) = dataRDD.cache().filterDivisor(line => line.startsWith("#"))
      val gatheredHeaderRDD = headerRDD.coalesce(1)
      val outputHeaderRDD = gatheredHeaderRDD.pipe(cmd)
      val outputVariantsRDD = variantsRDD.pipe(cmd).filter(line => !line.startsWith("#"))
      val finalResultRDD = outputHeaderRDD
        .union(outputVariantsRDD)
        .saveAsSingleTextFile(output_path)
    }
  }


  implicit class RDDOperators[T](rdd: RDD[T]) {
    // Split a RDD into 2 parts: 1 part which satisfies the condition and another part which does not satisfy
    def filterDivisor(f: T => Boolean): (RDD[T], RDD[T]) = {
      val passes = rdd.filter(f)
      val fails = rdd.filter(e => !f(e))
      (passes, fails)
    }
  }
}
