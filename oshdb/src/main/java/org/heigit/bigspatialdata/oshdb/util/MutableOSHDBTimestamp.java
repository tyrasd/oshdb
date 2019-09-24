package org.heigit.bigspatialdata.oshdb.util;

public interface MutableOSHDBTimestamp extends OSHDBTimestamp {
  void setTimestamp(long epoch);
}
