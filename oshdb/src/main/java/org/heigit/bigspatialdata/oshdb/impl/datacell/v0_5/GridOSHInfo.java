package org.heigit.bigspatialdata.oshdb.impl.datacell.v0_5;

import org.heigit.bigspatialdata.oshdb.datacell.DataCellInfo;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

public abstract class GridOSHInfo implements DataCellInfo {
  protected long id = -1;
  protected long level = -1;
  protected OSHDBBoundingBox bbox;
  
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getLevel() {
    return level;
  }

  public void setLevel(long level) {
    this.level = level;
  }
  
  @Override
  public OSHDBBoundingBox getBoundingBox() {
    return bbox;
  }
  
  public void setBBox(OSHDBBoundingBox bbox) {
    this.bbox = bbox;
    
  }
}
