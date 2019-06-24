package org.heigit.bigspatialdata.oshdb.tool.importer.util;

public class ProgressUtil {
  
  public static String hRBC(long bytes) {
    final int unit = 1024;
    if (bytes < unit)
      return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    final String pre = "" + "kMGTPE".charAt(exp - 1);
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

}
