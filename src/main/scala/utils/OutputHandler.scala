package utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD

import scala.util.Try

object OutputHandler {
  implicit class RDDExtensions(val rdd: RDD[String]) extends AnyVal {

    def saveAsSingleTextFile(path: String): Unit = saveAsSingleTextFileInternal(path, None)

    def saveAsSingleTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit =
      saveAsSingleTextFileInternal(path, Some(codec))

    // TODO: Optimise this function
    private def copyMerge(
                           srcFS: FileSystem, srcDir: Path,
                           dstFS: FileSystem, dstFile: Path,
                           deleteSource: Boolean, conf: Configuration
                         ): Boolean = {
      if (dstFS.exists(dstFile))
        throw new IOException(s"Target $dstFile already exists")
      // Source path is expected to be a directory:
      if (srcFS.getFileStatus(srcDir).isDirectory) {

        val outputFile = dstFS.create(dstFile)
        Try {
          srcFS.listStatus(srcDir)
            .sortBy(_.getPath.getName)
            .collect {
              case status if status.isFile =>
                val inputFile = srcFS.open(status.getPath)
                Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                inputFile.close()
            }
        }
        outputFile.close()

        if (deleteSource) srcFS.delete(srcDir, true) else true
      } else false
    }

    private def saveAsSingleTextFileInternal(
                                              path: String, codec: Option[Class[_ <: CompressionCodec]]
                                            ): Unit = {
      // The interface with hdfs:
      val hdfs = FileSystem.get(rdd.sparkContext.hadoopConfiguration)

      // Classic saveAsTextFile in a temporary folder:
      hdfs.delete(new Path(s"$path.tmp"), true) // to make sure it's not there already
      codec match {
        case Some(codec) => rdd.saveAsTextFile(s"$path.tmp", codec)
        case None        => rdd.saveAsTextFile(s"$path.tmp")
      }

      // Merge the folder of resulting part-xxxxx into one file:
      hdfs.delete(new Path(path), true) // to make sure it's not there already
      copyMerge(
        hdfs, new Path(s"$path.tmp"),
        hdfs, new Path(path),
        true, rdd.sparkContext.hadoopConfiguration
      )

      hdfs.delete(new Path(s"$path.tmp"), true)
    }
  }
}
