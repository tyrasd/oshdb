package org.heigit.bigspatialdata.oshdb.util.tagInterpreter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTags;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * instances of this class are used to determine whether a OSM way represents a polygon or linestring geometry.
 */
class BaseTagInterpreter implements TagInterpreter {
  int areaNoTagKeyId, areaNoTagValueId;
  Map<Integer, Set<Integer>> wayAreaTags;
  Map<Integer, Set<Integer>> relationAreaTags;
  Set<Integer> uninterestingTagKeys;
  int outerRoleId, innerRoleId, emptyRoleId;

  BaseTagInterpreter(
      int areaNoTagKeyId,
      int areaNoTagValueId,
      Map<Integer, Set<Integer>> wayAreaTags,
      Map<Integer, Set<Integer>> relationAreaTags,
      Set<Integer> uninterestingTagKeys,
      int outerRoleId,
      int innerRoleId,
      int emptyRoleId
  ) {
    this.areaNoTagKeyId = areaNoTagKeyId;
    this.areaNoTagValueId = areaNoTagValueId;
    this.wayAreaTags = wayAreaTags;
    this.relationAreaTags = relationAreaTags;
    this.uninterestingTagKeys = uninterestingTagKeys;
    this.outerRoleId = outerRoleId;
    this.innerRoleId = innerRoleId;
    this.emptyRoleId = emptyRoleId;
  }

  private boolean evaluateWayForArea(OSMWay entity) {
    List<OSHDBTag> tags = Lists.newArrayList(entity.getTags());
    if(OSHDBTags.hasTagValue(tags, areaNoTagKeyId, areaNoTagValueId))
      return false;
    for(OSHDBTag tag : tags){
      if (wayAreaTags.getOrDefault(tag.getIntKey(), Collections.emptySet()).contains(tag.getIntValue()))
        return true;
    }
    return false;
  }

  private boolean evaluateRelationForArea(OSMRelation entity) {
    for(OSHDBTag tag : entity.getTags()){
    // skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
      if(relationAreaTags.getOrDefault(tag.getIntKey(), Collections.emptySet()).contains(tag.getIntValue()))
        return true;
    }
    return false;
  }

  @Override
  public boolean isArea(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    } else if (entity instanceof OSMWay) {
      OSMWay way = (OSMWay) entity;
      OSHDBMember[] nds = Iterables.toArray(way.getMembers(), OSHDBMember.class);
      // must form closed ring with at least 3 vertices
      if (nds.length < 4 || nds[0].getId() != nds[nds.length - 1].getId()) {
        return false;
      }
      return this.evaluateWayForArea((OSMWay)entity);
    } else /*if (entity instanceof OSMRelation)*/ {
      return this.evaluateRelationForArea((OSMRelation)entity);
    }
  }

  @Override
  public boolean isLine(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    }
    return !isArea(entity);
  }

  @Override
  public boolean hasInterestingTagKey(OSMEntity osm) {
    for(OSHDBTag tag : osm.getTags()){
      if (!uninterestingTagKeys.contains(tag.getIntKey()))
        return true;
    }
    return false;
  }

  @Override
  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) {
    int outerWayCount = 0;

    for(OSHDBMember member: osmRelation.getMembers()){
      if (member.getType() == OSMType.WAY && member.getRole().getIntRole() == outerRoleId)
        if (++outerWayCount > 1) return false; // exit early if two outer ways were already found
    }
    if (outerWayCount != 1) return false;
    for(OSHDBTag tag : osmRelation.getTags()){
      if (relationAreaTags.getOrDefault(tag.getIntKey(), Collections.emptySet()).contains(tag.getIntValue()))
        continue;
      if (!uninterestingTagKeys.contains(tag.getIntKey()))
        return false;
    }
    return true;
  }

  @Override
  public boolean isMultipolygonOuterMember(OSHDBMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) return false;
    int roleId = osmMember.getRole().getIntRole();
    return roleId == this.outerRoleId ||
           roleId == this.emptyRoleId; // some historic osm data may still be mapped without roles set -> assume empty roles to mean outer
    // todo: check if there is need for some more clever outer/inner detection for the empty role case with old data
  }

  @Override
  public boolean isMultipolygonInnerMember(OSHDBMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) return false;
    return osmMember.getRole().getIntRole() == this.innerRoleId;
  }

}
