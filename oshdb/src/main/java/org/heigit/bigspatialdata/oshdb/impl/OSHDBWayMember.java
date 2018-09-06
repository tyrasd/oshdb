package org.heigit.bigspatialdata.oshdb.impl;

import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class OSHDBWayMember extends OSHDBMember {

  private OSHNode node = null;

  public OSHDBWayMember(long id) {
    this(id, null);
  }

  public OSHDBWayMember(long id, OSHNode node) {
    super(id);
    this.node = node;
  }
  
  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  public OSHNode getEntity() {
    return node;
  }

  public void setEntity(OSHNode node) {
    this.node = node;
  }

}
