package org.heigit.bigspatialdata.oshdb.osh;

import org.heigit.bigspatialdata.oshdb.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTags;

public interface OSHEntity extends Comparable<OSHEntity> {

  long getId();

  OSMType getType();
  
  

  OSHDBBoundingBox getBoundingBox();
  
  OSHDBTimestamp getFirstTimestamp();

  OSHDBTimestamp getLatestTimestamp();
  
  boolean isAlive();

  Iterable<OSHDBTag> getTags();
  
  default boolean hasTagKey(OSHDBTagKey tag) {
    return this.hasTagKey(tag.getIntKey());
  }

  default boolean hasTagKey(int key) {
    return OSHDBTags.hasTagKey(getTags(), key);
  }

  /**
   * Tests if any a given key is present but ignores certain values. Useful when looking for example
   * "TagKey" != "no"
   *
   * @param key the key to search for
   * @param uninterestingValues list of values, that should return false although the key is
   *        actually present
   * @return true if the key is present and is NOT in a combination with the given values, false
   *         otherwise
   */
  default boolean hasTagKeyExcluding(int key, int[] uninterestingValues) {
    return OSHDBTags.hasTagKeyExcluding(getTags(), key, uninterestingValues);
  }


  default boolean hasTagValue(int key, int value) {
    return OSHDBTags.hasTagValue(getTags(), key, value);
  }

  default int getTagValue(int key) {
    return OSHDBTags.getTagValue(getTags(), key);
  }
  
  Iterable<? extends OSMEntity> getVersions();
  
  Iterable<? extends OSHDBMember> getMembers();

  @Override
  default int compareTo(OSHEntity o) {
    int c = getType().compareTo(o.getType());
    return (c == 0) ? Long.compare(getId(), o.getId()) : c;
  }
  
}
