package util.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class CleanDir {
  public static boolean cleanHdfs(String path) throws IOException {

    Configuration conf = new Configuration();
    //handle "WRONG FS" exception
    try {
      FileSystem hdfs = FileSystem.get(conf);
      boolean res = hdfs.delete(new Path(path), true);
      return res;
    } catch (IllegalArgumentException e) {
      FileSystem hdfs = FileSystem.get(URI.create(path), conf);
      boolean res = hdfs.delete(new Path(path), true);
      return res;
    }

  }

  public static boolean cleanLocal(File path) {

    if (path.isDirectory()) {
      File[] listFiles = path.listFiles();
      for (int i = 0; i < listFiles.length && cleanLocal(listFiles[i]); i++) {
      }
    }
    return path.delete();
  }

  public static boolean clean(String path) throws IOException {
    String FileSystemId = path.split("://")[0];

    if (FileSystemId.equals("hdfs")) {
      return cleanHdfs(path);
    } else
      return cleanLocal(new File(path));
  }
}
