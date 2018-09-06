package org.heigit.bigspatialdata.oshdb;

import java.io.Serializable;
import java.util.Objects;

public class OSHDBTag implements Comparable<OSHDBTag>, Serializable {
  private static final long serialVersionUID = 1L;
  private int intKey;
  private int intValue;

  public OSHDBTag(int key, int value) {
    this.intKey = key;
    this.intValue = value;
  }

  public int getIntKey() {
    return this.intKey;
  }

  public int getIntValue() {
    return this.intValue;
  }

  public boolean isPresentInKeytables() {
    return this.intValue >= 0 && this.intKey >= 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBTag &&
        ((OSHDBTag)o).intKey == this.intKey && ((OSHDBTag)o).intValue == this.intValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.intKey, this.intValue);
  }

  @Override
  public String toString() {
    return Integer.toString(this.intKey) + "=" + Integer.toString(this.intValue);
  }
  
  @Override
  public int compareTo(OSHDBTag o) {
    int c = Integer.compare(intKey, o.intKey);
    if(c == 0)
      return Integer.compare(intValue, o.intValue);
    return c;
  }
}
