package org.heigit.bigspatialdata.oshdb.impl.osh;

import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

public abstract class MutableOSHWay extends MutableOSHRelation implements OSHWay {

  @Override
  public Iterable<? extends OSMWay> getVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
