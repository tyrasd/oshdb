package org.heigit.bigspatialdata.oshdb.impl.osh;

import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;

public abstract class MutableOSHNode extends MutableOSHEntity implements OSHNode {

  @Override
  public Iterable<? extends OSMNode> getVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
