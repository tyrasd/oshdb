package org.heigit.bigspatialdata.oshdb.notes;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class NoteComment {

  private final NoteAction action;
  private final OSHDBTimestamp timestamp;
  private final Integer uid;
  private String comment;

  public NoteComment(NoteAction action, OSHDBTimestamp timestamp, Integer uid, String comment) {
    this.action = action;
    this.timestamp = timestamp;
    this.uid = uid;
    this.comment = comment;
  }

  public NoteAction getAction() {
    return action;
  }

  //may contain all utf-8 and new lines
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public OSHDBTimestamp getTimestamp() {
    return timestamp;
  }

  public Integer getUid() {
    return uid;
  }

  @Override
  public String toString() {
    return "NoteComment{"
        + "action=" + action
        + ", timestamp=" + timestamp
        + ", uid=" + uid
        + ", comment=" + comment
        + '}';
  }

}
