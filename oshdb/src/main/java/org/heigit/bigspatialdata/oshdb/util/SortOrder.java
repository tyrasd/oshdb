package org.heigit.bigspatialdata.oshdb.util;

import java.util.Comparator;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public enum SortOrder {
  ASC(1), DESC(-1);

  public final int dir;
  public final Comparator<OSMEntity> sort;

  SortOrder(int dir) {
    this.dir = dir;
    sort = (a, b) -> {
      int c = a.getTimestamp().compareTo(b.getTimestamp());
      if (c == 0)
        c = Integer.compare(a.getVersion(), b.getVersion());
      return c * dir;
    };
  }

}
