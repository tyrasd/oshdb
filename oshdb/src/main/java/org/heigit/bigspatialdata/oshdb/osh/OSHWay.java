package org.heigit.bigspatialdata.oshdb.osh;

import org.heigit.bigspatialdata.oshdb.impl.OSHDBWayMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

public interface OSHWay extends OSHRelation {

  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }

  @Override
  Iterable<? extends OSMWay> getVersions();
  
  @Override
  Iterable<OSHDBWayMember> getMembers();
  
}
