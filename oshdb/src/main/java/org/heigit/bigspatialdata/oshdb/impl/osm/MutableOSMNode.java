package org.heigit.bigspatialdata.oshdb.impl.osm;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBTagList;

public class MutableOSMNode extends MutableOSMEntity implements OSMNode {

  private long longitude;
  private long latitude;

  public MutableOSMNode() {}

  public MutableOSMNode(long id, int version, boolean visible, long timestamp, long changeset,
      int userId, OSHDBTag[] tags, long lon, long lat) {
    super(id, version, visible, timestamp, changeset, userId, tags);
    this.longitude = lon;
    this.latitude = lat;

  }

  public MutableOSMNode(long id, int version, boolean visible, long timestamp, long changeset,
      int userId, OSHDBTagList tags, long lon, long lat) {
    super(id, version, visible, timestamp, changeset, userId, tags);
    this.longitude = lon;
    this.latitude = lat;
  }

  @Override
  public long getLon() {
    return longitude;
  }

  public void setLon(long longitude) {
    this.longitude = longitude;
  }

  @Override
  public long getLat() {
    return latitude;
  }

  public void setLat(long latitude) {
    this.latitude = latitude;
  }

  @Override
  public boolean equals(Object obj) {
    return equalsTo(obj);
  }

  @Override
  public String toString() {
    if (isVisible())
      return String.format("%s %d:%d", super.toString(), getLon(), getLat());
    return String.format("%s -:-", super.toString());
  }
}
