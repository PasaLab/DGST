import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class Configuration implements Serializable {

  Properties prop;
  String confLocation;

  int alphabetNum;
  int memBudget;
  int treeBudget;
  long fileBlockSize;
  String workingFolder;
  String inputFolder;

  public Configuration(String fileName) {
    confLocation = fileName;
    prop = new Properties();
    try {
      prop.load(Configuration.class.getClassLoader().getResourceAsStream(confLocation));
      memBudget = Integer.parseInt(prop.getProperty("memBudget"));
      treeBudget = Integer.parseInt(prop.getProperty("treeBudget"));
      fileBlockSize = Long.parseLong(prop.getProperty("fileBlockSize"));
      workingFolder = prop.getProperty("workingFolder");
      inputFolder = prop.getProperty("inputFolder");
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  public void set(String key, String value) {
    prop.setProperty(key, value);
  }

  public String get(String key) {
    return prop.getProperty(key);
  }

  public int getAlphabetNum() {
    return alphabetNum;
  }
}
