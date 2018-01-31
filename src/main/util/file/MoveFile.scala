package util.file

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object MoveFile {
  def apply(sur: String, dest: String): Unit = {
    sur.split("://")(0) match {
      case "hdfs" => moveHdfs(sur, dest)
      case _ => moveLocal(sur, dest)
    }
  }

  def moveHdfs(sur: String, dest: String): Unit = {
    try {
      val hdfs = FileSystem.get(new Configuration())
      if (hdfs.rename(new Path(sur), new Path(dest)))
        println("Successful")
      else
        println("Failed")
    }
    catch {
      case _ => println("exception")
    }
  }

  def moveLocal(sur: String, dest: String): Unit = {
    try {
      val oriFile = new File(sur)
      if (oriFile.renameTo(new File(dest)))
        println("Successful")
      else
        println("Failed")
    }
    catch {
      case _ => println("exception")
    }
  }
}
