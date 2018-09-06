package org.heigit.bigspatialdata.oshdb;

import java.io.Serializable;

public class OSHDBRole implements Comparable<OSHDBRole>, Serializable {
  private static final long serialVersionUID = 1L;
  private int intRole;

  public OSHDBRole(int role) {
    this.intRole = role;
  }

  public int getIntRole() {
    return this.intRole;
  }

  public boolean isPresentInKeytables() {
    return this.intRole >= 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBRole && ((OSHDBRole)o).intRole == this.intRole;
  }

  @Override
  public int hashCode() {
    return this.intRole;
  }

  @Override
  public String toString() {
    return Integer.toString(this.intRole);
  }

  @Override
  public int compareTo(OSHDBRole o) {
    return Integer.compare(intRole, o.intRole);
  }
}
