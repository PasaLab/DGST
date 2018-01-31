package kryo;

import com.esotericsoftware.kryo.Kryo;
import gct.rdd.GctConf;
import gct.rdd.smalltree.base.SmallTreeEdge;
import gct.rdd.smalltree.base.SmallTreeNode;
import org.apache.spark.serializer.KryoRegistrator;

import java.io.Serializable;
import java.lang.reflect.Array;

import javax.annotation.Nonnull;

/**
 * Register class for kyro serialization
 */
public class Register implements KryoRegistrator, Serializable {

  /**
   * register a class indicated by name
   *
   * @param kryo
   * @param s:   name of a class - might not exist
   */
  protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s) {
    Class c;
    try {
      c = Class.forName(s);
      doRegistration(kryo, c);
    } catch (ClassNotFoundException e) {
      return;
    }
  }

  /**
   * register a class
   *
   * @param kryo
   * @param pC   : input class - might not exist
   */
  protected void doRegistration(final Kryo kryo, final Class pC) {
    if (kryo != null) {
      kryo.register(pC);
      // also register arrays of that class
      Class arrayType = Array.newInstance(pC, 0).getClass();
      kryo.register(arrayType);
    }
  }

  /**
   * do the real work of registering all classes
   *
   * @param kryo
   */
  public void registerClasses(@Nonnull Kryo kryo) {
    kryo.register(Object[].class);
    kryo.register(scala.None$.class);
    kryo.register(scala.Tuple2[].class);
    kryo.register(scala.Tuple3[].class);
    kryo.register(int[].class);
    kryo.register(java.lang.Class.class);
    kryo.register(org.eclipse.collections.impl.map.mutable.UnifiedMap.class);
    kryo.register(String[].class);
    kryo.register(gct.rdd.divide.CollectInfo.class);
    kryo.register(gct.rdd.smalltree.LcpMergeSubTree.class);
    kryo.register(SmallTreeNode.class);
    kryo.register(SmallTreeEdge.class);
    kryo.register(gct.rdd.smalltree.SALcpForBuild[].class);
    kryo.register(gct.rdd.smalltree.SALcpForBuild.class);
    kryo.register(gct.rdd.EraTree.class);
    kryo.register(GctConf.class);
    kryo.register(gct.rdd.material.lcp.base.Player[][].class);
    kryo.register(gct.rdd.input.InputSplit[].class);
    kryo.register(gct.rdd.input.InputSplit.class);
    kryo.register(gct.rdd.input.FileSegment[].class);
    kryo.register(gct.rdd.input.FileSegment.class);
    kryo.register(scala.collection.Iterator[].class);
    doRegistration(kryo, "scala.collection.IndexedSeqLike$Elements");
    doRegistration(kryo, "scala.collection.mutable.ArrayOps$ofRef");

    doRegistration(kryo, "gct.rdd.material.lcp.base.Location");
    doRegistration(kryo, "gct.rdd.material.lcp.base.Location[]");
    doRegistration(kryo, "scala.reflect.ClassTag$$anon$1");
    doRegistration(kryo, "org.eclipse.collections.impl.list.mutable.FastList");
    doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
    doRegistration(kryo, "gct.rdd.material.lcp.base.Player");

  }

}
