package util.file

object FSWriterFactory {
  def apply(path: String): FSWriter = {
    path.split("://")(0) match {
      case "hdfs" => new HdfsWriter(path)
      case _ => new LocalWriter(path)
    }
  }
}
