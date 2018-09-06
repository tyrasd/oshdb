package org.heigit.bigspatialdata.oshdb.osm;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTags;
import com.google.common.collect.Iterables;

public interface OSMEntity extends Comparable<OSMEntity> {

  long getId();

  OSMType getType();

  int getVersion();

  OSHDBTimestamp getTimestamp();

  long getChangeset();

  int getUserId();

  boolean isVisible();

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

  Iterable<? extends OSHDBMember> getMembers();



  default boolean equalsTo(Object obj) {
    if (!(obj instanceof OSMEntity))
      return false;
    OSMEntity other = (OSMEntity) obj;

    boolean ret = (getId() == other.getId()) && (getVersion() == other.getVersion())
        && (isVisible() == other.isVisible())
        && (getTimestamp().getRawUnixTimestamp() == other.getTimestamp().getRawUnixTimestamp())
        && (getChangeset() == other.getChangeset()) && (getUserId() == other.getUserId())
        && (!isVisible() || Iterables.elementsEqual(getTags(), other.getTags())
            && (!isVisible() || Iterables.elementsEqual(getMembers(), other.getMembers())));
    return ret;
  }

  @Override
  default int compareTo(OSMEntity o) {
    int c = Long.compare(this.getId(), o.getId());
    if (c == 0) {
      c = Integer.compare(this.getVersion(), o.getVersion());
      if (c == 0) {
        c = this.getTimestamp().compareTo(o.getTimestamp());
      }
    }
    return c;
  }

}
