package org.heigit.bigspatialdata.oshdb.notes;

import java.util.List;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

//may also be called OSHNote containing OSMNotes being the comments
public class OSMNote {

  private final int id;
  private final long latitude;
  private final long longitude;
  private final OSHDBTimestamp created;
  private final OSHDBTimestamp closed;
  private List<NoteComment> comments;

  public OSMNote(
      int id,
      long lat,
      long lon,
      OSHDBTimestamp created,
      OSHDBTimestamp closed,
      List<NoteComment> comments) {
    this.id = id;
    this.longitude = lon;
    this.latitude = lat;
    this.created = created;
    this.closed = closed;
    this.comments = comments;
  }

  public OSHDBTimestamp getClosed() {
    return closed;
  }

  public List<NoteComment> getComments() {
    return comments;
  }

  public void setComments(List<NoteComment> comments) {
    this.comments = comments;
  }

  public OSHDBTimestamp getCreated() {
    return created;
  }

  public int getId() {
    return id;
  }

  public long getLat() {
    return latitude;
  }

  public long getLon() {
    return longitude;
  }

  public double getLatitude() {
    return latitude * OSHDB.GEOM_PRECISION;
  }

  public double getLongitude() {
    return longitude * OSHDB.GEOM_PRECISION;
  }

  @Override
  public String toString() {
    return "OSMNote{"
        + "id=" + id
        + ", latitude=" + latitude
        + ", longitude=" + longitude
        + ", created=" + created
        + ", closed=" + closed
        + ", comments=" + comments
        + '}';
  }

}
