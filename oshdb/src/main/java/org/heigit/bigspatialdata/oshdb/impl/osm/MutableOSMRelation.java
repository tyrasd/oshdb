package org.heigit.bigspatialdata.oshdb.impl.osm;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBMemberList;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBTagList;
import com.google.common.collect.Iterables;

public class MutableOSMRelation extends MutableOSMEntity implements OSMRelation {

  protected OSHDBMemberList members = new OSHDBMemberList();

  public MutableOSMRelation() {}


  public MutableOSMRelation(long id, int version, boolean visible, long timestamp, long changeset, int userId, OSHDBTag[] tags, OSHDBMember[] members) {
    super(id, version, visible, timestamp, changeset, userId, tags);
    this.members.addElements(0, members);
  }


  public MutableOSMRelation(long id, int version, boolean visible, long timestamp, long changeset, int userId, OSHDBTagList tags, OSHDBMemberList members) {
    super(id, version, visible, timestamp, changeset, userId, tags);
    this.members.set(members);
  }



  public void reset(long id, int version, long timestamp) {
    super.reset(id, version, timestamp);
    members.clear();
  }

  @Override
  public OSHDBMemberList getMembers() {
    return members;
  }

  @Override
  public String toString() {
    return String.format("%s members:%s", super.toString(), Iterables.toString(getMembers()));
  }


}
