package org.heigit.bigspatialdata.oshdb.impl;

import org.heigit.bigspatialdata.oshdb.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class OSHDBRelationMember extends OSHDBMember {

  private OSMType type;
  private OSHDBRole role;
  private OSHEntity entity = null;

  public OSHDBRelationMember(long id, OSMType type, int role) {
    super(id);
    this.type = type;
    this.role = new OSHDBRole(role);
  }

  public OSHDBRelationMember(OSHEntity entity, int role) {
    super(entity.getId());
    this.type = entity.getType();
    this.entity = entity;
    this.role = new OSHDBRole(role);
  }

  public OSHDBRelationMember(OSHDBMember member) {
    super(member.getId());
    if (member.getEntity() != null) {
      this.type = member.getEntity().getType();
      this.entity = member.getEntity();
    } else {
      this.type = member.getType();
    }
    this.role = new OSHDBRole(member.getRole().getIntRole());
  }


  @Override
  public OSMType getType() {
    return type;
  }


  public void setType(OSMType type) {
    this.type = type;
  }

  @Override
  public OSHEntity getEntity() {
    return entity;
  }

  public void setEntity(OSHEntity entity) {
    this.entity = entity;
  }

  @Override
  public OSHDBRole getRole() {
    return role;
  }


  public void setRole(OSHDBRole role) {
    this.role = role;
  }
}
