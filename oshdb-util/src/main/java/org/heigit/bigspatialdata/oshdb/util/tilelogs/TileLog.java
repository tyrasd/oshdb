package org.heigit.bigspatialdata.oshdb.util.tilelogs;

import java.time.Instant;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;

public class TileLog {

  private int zoom;
  private int x;
  private int y;
  private Map<OSHDBTimestampInterval, Integer> log;

  public TileLog(int zoom, int x, int y) {
    this.zoom = zoom;
    this.x = x;
    this.y = y;
    this.log = new HashMap<>();
  }

  public OSHDBBoundingBox getBbox() {
// code from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Tile_bounding_box
    double north = tile2lat(y, zoom);
    double south = tile2lat(y + 1, zoom);
    double west = tile2lon(x, zoom);
    double east = tile2lon(x + 1, zoom);
    return new OSHDBBoundingBox(west, south, east, north);
  }

  private static double tile2lon(int x, int z) {
    return x / Math.pow(2.0, z) * 360.0 - 180;
  }

  private static double tile2lat(int y, int z) {
    double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
    return Math.toDegrees(Math.atan(Math.sinh(n)));
  }

  public Map<OSHDBTimestampInterval, Integer> getLog() {
    return log;
  }

  public void addLog(OSHDBTimestamp from, int count) {
    Instant plus = from.toDate().toInstant().plus(Period.ofDays(1));
    OSHDBTimestamp to = new OSHDBTimestamp(plus.getEpochSecond());
    OSHDBTimestampInterval interval = new OSHDBTimestampInterval(from, to);
    this.log.put(interval, count);
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  public int getZoom() {
    return zoom;
  }

  @Override
  public String toString() {
    return "TileLog{"
        + "zoom=" + zoom
        + ", x=" + x
        + ", y=" + y
        + ", log=" + log
        + ", bbox=" + this.getBbox()
        + '}';
  }

}
