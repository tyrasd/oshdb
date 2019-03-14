package org.heigit.bigspatialdata.oshdb.datacell;

import java.io.IOException;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;

/**
 * interface for oshdb partition
 * 
 *
 */
public interface DataCell {
  
  DataCellInfo getInfo();
  
  Iterable<? extends OSHEntity> getEntities() throws IOException;

}
