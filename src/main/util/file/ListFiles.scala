package util.file

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ListFiles extends Serializable {
  def apply(path: String) = path.split("://")(0) match {
    case "hdfs" => {
      val fs = try {
        val hdfs = FileSystem.get(new Configuration())
        hdfs.listStatus(new Path(path)).filter(x => !x.isDirectory)
      }
      catch {
        case e: IllegalArgumentException => {
          val hdfs = FileSystem.get(URI.create(path), new Configuration())
          hdfs.listStatus(new Path(path)).filter(x => !x.isDirectory)
        }
      }

      val names = fs.map(x => x.getPath.toString)
      names
    }
    case _ => {
      val root = new File(path)
      val files = root.listFiles().filter(x => !x.isDirectory)
      val names = files.map(x => x.getAbsolutePath())
      names
    }
  }
}
