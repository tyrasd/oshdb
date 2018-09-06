package org.heigit.bigspatialdata.oshdb.osh;

import java.util.Collections;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSHNode extends OSHEntity {

  @Override
  default OSMType getType() {
    return OSMType.NODE;
  } 

  @Override
  Iterable<? extends OSMNode> getVersions();
  
  @Override
  default Iterable<OSHDBMember> getMembers() {
    return Collections.emptyList();
  }
  
}
