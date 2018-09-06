package org.heigit.bigspatialdata.oshdb;

import java.util.Objects;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public abstract class OSHDBMember implements Comparable<OSHDBMember> {

  private static final OSHDBRole NO_ROLE = new OSHDBRole(-1);

  private long id;

  protected OSHDBMember(long id){
    this.id = id;
  }
  
  public long getId() {
    return id;
  };

  public abstract OSMType getType();

  public OSHDBRole getRole() {
    return NO_ROLE;
  }

  public OSHEntity getEntity() {
    return null;
  }

  @Override
  public int compareTo(OSHDBMember o) {
    int c = Long.compare(getId(), o.getId());
    if (c == 0) {
      c = getType().compareTo(o.getType());
      if (c == 0)
        return getRole().compareTo(o.getRole());
    }
    return c;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getType(), getRole());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OSHDBMember))
      return false;
    OSHDBMember other = (OSHDBMember) obj;
    return this.getId() == other.getId() && this.getType() == other.getType()
        && this.getRole().equals(other.getRole());
  }

}
