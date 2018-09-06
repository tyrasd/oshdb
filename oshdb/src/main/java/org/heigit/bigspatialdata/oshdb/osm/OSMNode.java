package org.heigit.bigspatialdata.oshdb.osm;

import java.util.Collections;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;

public interface OSMNode extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.NODE;
  }

  long getLon();

  default double getLongitude() {
    return OSHDB.longToDouble(getLon());
  }

  long getLat();

  default double getLatitude() {
    return OSHDB.longToDouble(getLat());
  }

  @Override
  default Iterable<OSHDBMember> getMembers() {
    return Collections.emptyList();
  }
}
