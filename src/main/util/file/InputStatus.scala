package util.file

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object InputStatus {

  def getLength(path: String): Long = {
    path.split("://")(0) match {
      case "hdfs" => {
        val fs = try {
          val hdfs = FileSystem.get(new Configuration())
          hdfs.getFileStatus(new Path(path))
        }
        catch {
          case e: IllegalArgumentException => {
            val hdfs = FileSystem.get(URI.create(path), new Configuration())
            hdfs.getFileStatus(new Path(path))
          }
        }
        fs.getLen
      }
      case _ => new File(path).length()
    }
  }
}
