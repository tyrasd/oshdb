package org.heigit.bigspatialdata.oshdb.osm;

public interface OSMRelation extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.RELATION;
  }

  
}
