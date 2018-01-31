package util.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class CreateDir {
  public static boolean createLocal(String path) {
    File dir = new File(path);
    if (dir.exists()) {
      System.out.println(path + ", already exists.");
      return false;
    }
    if (!path.endsWith(File.separator)) {
      path = path + File.separator;
    }

    if (dir.mkdirs()) {
      System.out.println("Create " + path + "succeed.");
      return true;
    } else {
      System.out.println("Create " + path + "fail.");
      return false;
    }
  }

  public static boolean createHdfs(String path) throws IOException {
    Configuration conf = new Configuration();
    try {
      FileSystem hdfs = FileSystem.get(conf);
      Path dfs = new Path(path);
      if (hdfs.mkdirs(dfs)) {
        System.out.println("Create " + path + "succeed.");
        return true;
      } else {
        System.out.println("Create " + path + "fail.");
        return false;
      }
    } catch (IllegalArgumentException e) {
      FileSystem hdfs = FileSystem.get(URI.create(path), conf);
      Path dfs = new Path(path);
      if (hdfs.mkdirs(dfs)) {
        System.out.println("Create " + path + "succeed.");
        return true;
      } else {
        System.out.println("Create " + path + "fail.");
        return false;
      }
    }

  }

  public static boolean create(String path) throws IOException {
    String FileSystemId = path.split("://")[0];

    if (FileSystemId.equals("hdfs")) {
      return createHdfs(path);
    } else
      return createLocal(path);
  }
}
