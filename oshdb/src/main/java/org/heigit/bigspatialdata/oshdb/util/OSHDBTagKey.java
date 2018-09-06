package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;

public class OSHDBTagKey implements Serializable {
  private static final long serialVersionUID = 1L;
  private int intKey;

  public OSHDBTagKey(int key) {
    this.intKey = key;
  }

  public int getIntKey() {
    return this.intKey;
  }

  public boolean isPresentInKeytables() {
    return this.intKey >= 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBTagKey && ((OSHDBTagKey)o).intKey == this.intKey;
  }

  @Override
  public int hashCode() {
    return this.intKey;
  }

  @Override
  public String toString() {
    return Integer.toString(this.intKey);
  }
}
