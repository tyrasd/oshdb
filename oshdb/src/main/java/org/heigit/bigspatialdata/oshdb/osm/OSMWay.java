package org.heigit.bigspatialdata.oshdb.osm;

public interface OSMWay extends OSMRelation {

  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }
}
