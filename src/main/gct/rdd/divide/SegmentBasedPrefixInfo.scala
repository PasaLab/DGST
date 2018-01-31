package gct.rdd.divide

import gct.rdd.material.lcp.base.SharedBufferSuffix
import org.eclipse.collections.impl.list.mutable.FastList

/**
 * S-prefix information based on segment
 */
case class SegmentBasedPrefixInfo(ind: Int, len: Int, loc: FastList[SharedBufferSuffix] = FastList.newList[SharedBufferSuffix]())
