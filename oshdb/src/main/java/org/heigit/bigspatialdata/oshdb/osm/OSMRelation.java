package org.heigit.bigspatialdata.oshdb.osm;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntities;

public interface OSMRelation extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.RELATION;
  }

  
}
