package org.heigit.bigspatialdata.oshdb;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.util.MutableOSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public abstract class OSHDB {

  public static final int MAXZOOM = 15;

  // osm only stores 7 decimals for each coordinate
  public static final long GEOM_PRECISION_TO_LONG = 10000000L;
  public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;

  /**
   * Returns various metadata properties of this OSHDB instance
   *
   * For example, metadata("extract.region") returns the geographic region for which the current
   * oshdb extract has been generated in GeoJSON format.
   *
   * @param property the metadata property to request
   * @return the value of the requested metadata field
   */
  public abstract String metadata(String property);


  public static OSHDBTimestamp timestamp(long epoch) {
    return new Timestamp(epoch);
  }

  public static MutableOSHDBTimestamp mutableTimestamp(long epoch) {
    return new Timestamp(epoch);
  }

  public static class Timestamp implements MutableOSHDBTimestamp {
    private static final long serialVersionUID = 1L;
    private long _tstamp;

    public Timestamp(long tstamp) {

      this._tstamp = tstamp;
    }

    public Timestamp(Date tstamp) {
      this(tstamp.getTime() / 1000);
    }

    public void setTimestamp(long tstamp) {
      this._tstamp = tstamp;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof OSHDBTimestamp) {
        return compareTo((OSHDBTimestamp) other) == 0;
      }
      return false;
    }

    public Date toDate() {
      return new Date(this._tstamp * 1000);
    }

    public long getRawUnixTimestamp() {
      return this._tstamp;
    }

    public String toString() {
      ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(_tstamp), ZoneOffset.UTC);
      return zdt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
  }
}
