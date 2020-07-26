package org.usama.aidrecommender.utils

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException
import java.net.URI

import org.apache.spark.sql.SparkSession

/**
  * Utility class to merge part-0000.* files into a single file
  */
class MyFileUtil {

  /**
    * @param srcFS source Filesystem
    * @param srcDir source Dir
    * @param dstFS Filesystem Hdfs/S3 etc
    * @param dstFile filename
    * @param deleteSource if true will delte the sourceDir
    * @param overwrite if true will overwrite the file
    * @param conf hadoop args
    * @return
    */
  private def copyMerge(
      srcFS: FileSystem,
      srcDir: Path,
      dstFS: FileSystem,
      dstFile: Path,
      deleteSource: Boolean,
      overwrite: Boolean,
      conf: Configuration,
      header: String
  ): Boolean = {

    if (!overwrite && dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile, overwrite)
      if (!header.isEmpty) outputFile.write((header + "\n").getBytes("UTF-8"))
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    } else false
  }

  /**
    * Since FileUtil.copyMerge has been deprecated and we're using -Xfatal-warnings in combination with -deprecation for ScalaOptions
    * which generates warning for deprecated code and fails to compile if there's a warning we write our own method above copyMerge
    */
  def merge(
      spark: SparkSession,
      srcPath: String,
      dstPath: String,
      header: String = "",
      deleteSource: Boolean = true,
      overwrite: Boolean = true
  ): Unit = {
    val config         = spark.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(new URI(srcPath), config)
    copyMerge(
      fs,
      new Path(srcPath),
      fs,
      new Path(dstPath),
      deleteSource,
      overwrite,
      config,
      header
    )
  }

}
