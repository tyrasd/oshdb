package org.heigit.bigspatialdata.oshdb.util;

/**
 * A wrapper class combining Metadata of the used OSHDB.
 */
public class OSHDBMetadata {

  public static final OSHDBMetadata INVALID
      = new OSHDBMetadata(new OSHDBTimestamp(0), new OSHDBTimestamp(0), OSHDBBoundingBox.EMPTY);

  private final OSHDBBoundingBox bbox;
  private final OSHDBTimestamp endTime;
  private final OSHDBTimestamp startTime;

  /**
   * Creates a new Metadata-Object with data read from the OSHDB.
   *
   * @param bbox the BoundingBox of the data
   * @param endTime the up-to-dateness of the data.
   * @param startTime the valid-from timestamp of the OSHDB
   */
  public OSHDBMetadata(
      OSHDBTimestamp startTime,
      OSHDBTimestamp endTime,
      OSHDBBoundingBox bbox
  ) {
    this.bbox = bbox;
    this.endTime = endTime;
    this.startTime = startTime;
  }

  /**
   * Get the Data-Bbox-
   *
   * @return the data-bbox
   */
  public OSHDBBoundingBox getBbox() {
    return bbox;
  }

  /**
   * Get the up-to-dateness.
   *
   * @return the valid-to timestamp
   */
  public OSHDBTimestamp getEndTime() {
    return endTime;
  }

  /**
   * Get the hystory-timestmap.
   *
   * @return the valid-from timestamp
   */
  public OSHDBTimestamp getStartTime() {
    return startTime;
  }

}
