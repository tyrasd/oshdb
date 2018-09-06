package org.heigit.bigspatialdata.oshdb.impl.osh;

import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;

public abstract class MutableOSHRelation extends MutableOSHEntity implements OSHRelation {

  @Override
  public Iterable<? extends OSMRelation> getVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
