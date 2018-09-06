package org.heigit.bigspatialdata.oshdb.osh;

import com.google.common.collect.Lists;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.impl.OSHDBWayMember;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

public interface OSHWay extends OSHRelation {

  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }

  @Override
  Iterable<? extends OSMWay> getVersions();
  
  @Override
  Iterable<OSHDBWayMember> getMembers();
  
}
