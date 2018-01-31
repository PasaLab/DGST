package util.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

public interface FSReader {
  int read(int pos, byte[] buf) throws IOException;

  int read(byte[] buf) throws IOException;

  void seek(Long len) throws IOException;
}


class LocalReader implements FSReader {
  FileInputStream in;

  LocalReader(String path) throws FileNotFoundException {
    in = new FileInputStream(path);
  }

  public int read(int pos, byte[] buf) throws IOException {
    seek(pos);
    return in.getChannel().read(ByteBuffer.wrap(buf));
  }

  public int read(byte[] buf) throws IOException {
    return in.getChannel().read(ByteBuffer.wrap(buf));
  }

  public void seek(Long len) throws IOException {
    in.getChannel().position(len);
  }

  public void seek(int len) throws IOException {
    seek(new Long(len));
  }
}


class HdfsReader implements FSReader {
  FSDataInputStream in;

  HdfsReader(String path) throws IOException {
    Configuration hdfsConf = new Configuration();
    try {
      FileSystem hdfs = FileSystem.get(hdfsConf);
      in = hdfs.open(new Path(path));
    } catch (IllegalArgumentException e) {
      FileSystem hdfs = FileSystem.get(URI.create(path), hdfsConf);
      in = hdfs.open(new Path(path));
    }
  }

  public int read(int pos, byte[] buf) throws IOException {
    return in.read(new Long(pos), buf, 0, buf.length);
  }

  public int read(byte[] buf) throws IOException {
    return in.read(buf);
  }

  public void seek(Long len) throws IOException {
    in.seek(len);
  }
}
