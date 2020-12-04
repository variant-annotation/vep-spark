import extprg.annovar.ANNOVAR.annotateByAnnovar
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import utils.ArgumentOption
import extprg.vep.VEP._
import scala.collection.mutable.ArrayBuffer

object VASparkApplication {
  // TODO: Write a new usage
  val usage = """
    spark-submit vaspark.jar [spark-args] [vaspark-args] [external-program-args]
  """
  def main(args: Array[String]): Unit = {
    // Set log level to STDERR
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    // Initialize Spark Application
    val spark = SparkSession.builder()
      .appName("VASpark Application")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Handle arguments
    val extPrgArgs: ArrayBuffer[ArgumentOption] = new ArrayBuffer[ArgumentOption]
    val argsList = args.toList
    type OptionMap = Map[String, Any]
    @scala.annotation.tailrec
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isValue(s : String) = (s(0) != '-')
      list match {
        case Nil => map
        case "--tool" :: value :: tail =>
          nextOption(map ++ Map("tool" -> value), tail)
        case "--exec_dir" :: value :: tail =>
          nextOption(map ++ Map("exec_dir" -> value), tail)
        case currentParam :: nextParam :: tail if isValue(nextParam) =>
          extPrgArgs += new ArgumentOption(currentParam, nextParam, true)
          nextOption(map, tail)
        case currentParam :: nextParam :: tail if !isValue(nextParam) =>
          extPrgArgs += new ArgumentOption(currentParam, null, false)
          nextOption(map, list.tail)
        case param :: Nil =>
          extPrgArgs += new ArgumentOption(param, null, false)
          nextOption(map, list.tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }
    val vasparkArgs = nextOption(Map(),argsList)

    if (
      !vasparkArgs.contains("tool") ||
      !vasparkArgs.contains("exec_dir")
    ) {
      print(usage)
    } else {
      vasparkArgs.get("tool").mkString match {
        case "vep" => annotateByVep(
          sc,
          extPrgArgs.toArray,
          vasparkArgs.get("exec_dir").mkString
        )
        case "annovar" => annotateByAnnovar(
          sc,
          extPrgArgs.toArray,
          vasparkArgs.get("exec_dir").mkString
        )
      }
    }
  }
}
