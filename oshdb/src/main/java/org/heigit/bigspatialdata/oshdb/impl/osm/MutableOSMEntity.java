package org.heigit.bigspatialdata.oshdb.impl.osm;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBTagList;
import com.google.common.collect.Iterables;

public abstract class MutableOSMEntity implements OSMEntity {

  private long id;
  private int version;
  private boolean visible;
  private OSHDBTimestamp timestamp = new OSHDBTimestamp(0L);
  private long changeset;
  private int userId;
  private OSHDBTagList tags = new OSHDBTagList();


  public MutableOSMEntity() {}

  public MutableOSMEntity(long id, int version, boolean visible, long timestamp, long changeset,
      int userId) {
    this.id = id;
    this.version = version;
    this.visible = visible;
    this.timestamp.setTimestamp(timestamp);
    this.changeset = changeset;
    this.userId = userId;
  }

  public MutableOSMEntity(long id, int version, boolean visible, long timestamp, long changeset,
      int userId, OSHDBTag[] tags) {
    this(id, version, visible, timestamp, changeset, userId);
    this.tags.addElements(0, tags);
  }

  public MutableOSMEntity(long id, int version, boolean visible, long timestamp, long changeset,
      int userId, OSHDBTagList tags) {
    this(id, version, visible, timestamp, changeset, userId);
    this.tags.set(tags);
  }

  public void reset(long id, int version, long timestamp) {
    this.id = id;
    this.version = version;
    this.timestamp.setTimestamp(timestamp);
    this.visible = true;
    this.changeset = 0;
    this.userId = 0;
    this.tags.clear();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public boolean isVisible() {
    return visible;
  }

  public void setVisible(boolean visible) {
    this.visible = visible;
  }

  public OSHDBTimestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(OSHDBTimestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp.setTimestamp(timestamp);
  }

  public long getChangeset() {
    return changeset;
  }

  public void setChangeset(long changeset) {
    this.changeset = changeset;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }


  public void setTags(OSHDBTagList other) {
    this.tags.set(other);
  }

  public void setTags(OSHDBTag[] tags, int length) {
    this.tags.clear();
    this.tags.addElements(0, tags, 0, length);
  }

  public void setTags(Iterable<OSHDBTag> tags) {
    this.tags.clear();
    this.tags.set(tags);
  }

  public OSHDBTagList getTags() {
    return this.tags;
  }

  @Override
  public String toString() {
    return String.format("%s(ID:%d V:%d TS:%d CS:%d VIS:%s UID:%d TAGS:%S)", getType(), getId(),
        getVersion(), getTimestamp().getRawUnixTimestamp(), getChangeset(), isVisible(),
        getUserId(), Iterables.toString(getTags()));
  }

  @Override
  public boolean equals(Object obj) {
    return equalsTo(obj);
  }

}
