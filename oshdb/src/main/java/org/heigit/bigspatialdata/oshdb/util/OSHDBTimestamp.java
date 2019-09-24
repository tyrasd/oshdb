package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;

public interface OSHDBTimestamp extends Comparable<OSHDBTimestamp>, Serializable {
  
  /**
   * 
   * @return timestamp in seconds since January 1, 1970 UTC.
   */
  long getRawUnixTimestamp();
  
  @Override
  default int compareTo(OSHDBTimestamp o) {
    System.currentTimeMillis();
    return Long.compare(getRawUnixTimestamp(), o.getRawUnixTimestamp());
  }
}
