package org.heigit.bigspatialdata.oshdb.osm;

public enum OSMType {
  UNKNOWN(-1),
  NODE(0),
  WAY(1),
  RELATION(2);

  private final int type;

  OSMType(final int value) {
    this.type = value;
  }

  public static OSMType fromInt(final int value) {
    switch(value) {
      case 0:
        return NODE;
      case 1:
        return WAY;
      case 2:
        return RELATION;
      default:
        return UNKNOWN;
    }
    
  }

  public final int intType() {
    return this.type;
  }
}
