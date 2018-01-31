package util.file;

import gct.rdd.GctConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

public interface FSWriter extends Serializable {
  void write(String s) throws IOException;

  void write(byte s[]) throws IOException;

  void write(byte s[], int start, int len) throws IOException;

  void close() throws IOException;

  void open(String s) throws IOException;
}


class HdfsWriter implements FSWriter {

  Configuration conf;
  FileSystem fs;
  volatile FSDataOutputStream out;

  public HdfsWriter(String path) throws IOException {
    try {
      conf = new Configuration();
      fs = FileSystem.get(URI.create(path), conf);
    } catch (Exception e) {
      throw new RuntimeException("failed to create hdfs write ", e);
    }
    if (out != null) {
      throw new RuntimeException("out should be null");
    }
  }

  public void open(String path) throws IOException {
    out = fs.create(new Path(path));
  }

  public void write(String s) throws IOException {
    if (out == null) {
      throw new RuntimeException("out is null");
    }
    out.write(s.getBytes());
    out.flush();
  }

  public void write(byte[] s) throws IOException {
    out.write(s);
    out.flush();
  }

  public void write(byte[] s, int start, int len) throws IOException {
    out.write(s, start, len);
    out.flush();
  }

  public void close() throws IOException {
    out.close();
    out = null;
  }
}


class LocalWriter implements FSWriter {

  GctConf conf = new GctConf();
  FileOutputStream outStr;
  BufferedOutputStream buf;

  public LocalWriter(String path) throws IOException {

    if (path.indexOf(conf.dirSpliter()) > 0) {
      String dir;
      dir = path.substring(0, path.lastIndexOf(conf.dirSpliter()));
      CreateDir.create(dir);
    } else {
      CreateDir.create(path);
    }
  }

  public void open(String path) throws FileNotFoundException {
    outStr = new FileOutputStream(path);
    buf = new BufferedOutputStream(outStr);
  }

  public void write(String s) throws IOException {
    buf.write(s.getBytes());
    buf.flush();
  }

  public void write(byte[] s) throws IOException {
    buf.write(s);
    buf.flush();
  }

  public void write(byte[] s, int start, int len) throws IOException {
    buf.write(s, start, len);
    buf.flush();
  }

  public void close() throws IOException {
    buf.close();
  }
}
