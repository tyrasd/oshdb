package org.heigit.bigspatialdata.oshdb.partition;

import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

public interface PartitionInfo {
  
  OSHDBBoundingBox getBoundingBox();

}
