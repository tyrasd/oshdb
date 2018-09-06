package org.heigit.bigspatialdata.oshdb.impl.osm;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBMemberList;
import org.heigit.bigspatialdata.oshdb.util.collections.OSHDBTagList;

public class MutableOSMWay extends MutableOSMRelation implements OSMWay {

  public MutableOSMWay() {}

  public MutableOSMWay(long id, int version, boolean visible, long timestamp, long changeset, int userId, OSHDBTag[] tags, OSHDBMember[] members) {
    super(id, version, visible, timestamp, changeset, userId, tags, members);
  }
  
  public MutableOSMWay(long id, int version, boolean visible, long timestamp, long changeset, int userId, OSHDBTagList tags, OSHDBMemberList members) {
    super(id, version, visible, timestamp, changeset, userId, tags, members);
  }

}
