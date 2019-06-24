package org.heigit.bigspatialdata.oshdb;

import org.heigit.bigspatialdata.oshdb.util.OSHDBMetadata;

public abstract class OSHDB {

  public static final int MAXZOOM = 15;

  // osm only stores 7 decimals for each coordinate
  public static final long GEOM_PRECISION_TO_LONG = 10000000L;
  public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;

  /**
   * Returns various metadata properties of this OSHDB instance
   *
   *
   * @return the org.heigit.bigspatialdata.oshdb.util.OSHDBMetadata
   */
  public abstract OSHDBMetadata getMetadata();
}
