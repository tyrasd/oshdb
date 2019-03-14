package org.heigit.bigspatialdata.oshdb.partition;

import java.io.IOException;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;

/**
 * interface for oshdb partition
 * 
 *
 */
public interface Partition {
  
  PartitionInfo getInfo();
  
  Iterable<? extends OSHEntity> getEntities() throws IOException;

}
