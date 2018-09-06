package org.heigit.bigspatialdata.oshdb.util;

import com.google.common.base.Predicates;
import com.google.common.collect.Streams;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.impl.OSHDBWayMember;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntities;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

public abstract class OSHDBMembers {

  public static Stream<OSMNode> getMemberBy(OSMWay osm, OSHDBTimestamp timestamp,
      Predicate<OSHDBWayMember> memberFilter) {
    return getWayMemberBy(osm, timestamp, memberFilter);
  }

  private static Stream<OSMNode> getWayMemberBy(OSMWay osm, OSHDBTimestamp timestamp,
      Predicate<OSHDBWayMember> memberFilter) {
    return Streams.stream(osm.getMembers()).filter(m -> m instanceof OSHDBWayMember)
        .map(m -> (OSHDBWayMember) m).filter(memberFilter).map(OSHDBWayMember::getEntity)
        .filter(Objects::nonNull).map(entity -> OSHEntities.getByTimestamp(entity, timestamp));
  }

  public static Stream<OSMNode> getMemberByTimestamp(OSMWay osm, OSHDBTimestamp timestamp) {
    return getWayMemberBy(osm, timestamp, Predicates.alwaysTrue());
  }

  public static Stream<OSMEntity> getMemberBy(OSMEntity osm, OSHDBTimestamp timestamp,
      Predicate<OSHDBMember> memberFilter) {
    return Streams.stream(osm.getMembers()).filter(memberFilter).map(OSHDBMember::getEntity)
        .filter(Objects::nonNull).map(e -> OSHEntities.getByTimestamp(e.getVersions(), timestamp));
  }

  public static Stream<OSMEntity> getMemberByTimestamp(OSMEntity osm, OSHDBTimestamp timestamp) {
    return getMemberBy(osm, timestamp, Predicates.alwaysTrue());
  }

}
