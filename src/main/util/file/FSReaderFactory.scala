package util.file

object FSReaderFactory {
  def apply(path: String): FSReader = {
    path.split("://")(0) match {
      case "hdfs" => new HdfsReader(path)
      case _ => new LocalReader(path)
    }
  }
}
