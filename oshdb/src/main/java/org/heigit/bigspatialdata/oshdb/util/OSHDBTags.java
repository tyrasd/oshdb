package org.heigit.bigspatialdata.oshdb.util;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;

public abstract class OSHDBTags {
  private static final int[] EMPTY = new int[0];
  
  public static boolean hasTagKey(Iterable<OSHDBTag> tags, int key) {
    return hasTagKeyExcluding(tags, key, EMPTY);
  }

  public static boolean hasTagKeyExcluding(Iterable<OSHDBTag> tags, int key, int[] uninterestingValues) {
    for (OSHDBTag tag : tags) {
      final int tagKey = tag.getIntKey();
      final int tagValue = tag.getIntValue();
      if (tagKey < key)
        continue;

      if (tagKey == key) {
        for (int u = 0; u < uninterestingValues.length; u++) {
          if (tagValue == uninterestingValues[u])
            return false;
        }
        return true;
      }

      if (tagKey > key)
        break;
    }
    return false;
  }

  public static boolean hasTagValue(Iterable<OSHDBTag> tags, int key, int value) {
    int tagValue = getTagValue(tags, key);
    return tagValue == value;
  }

  public static int getTagValue(Iterable<OSHDBTag> tags, int key) {
    for (OSHDBTag tag : tags) {
      final int tagKey = tag.getIntKey();
      final int tagValue = tag.getIntValue();
      if (tagKey < key)
        continue;

      if (tagKey == key) {
        return tagValue;
      }

      if (tagKey > key)
        break;
    }
    return -1;
  }
}
