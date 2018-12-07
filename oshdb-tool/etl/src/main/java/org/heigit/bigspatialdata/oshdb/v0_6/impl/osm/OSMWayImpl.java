package org.heigit.bigspatialdata.oshdb.v0_6.impl.osm;

import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMWay;

public class OSMWayImpl extends OSMEntityImpl implements OSMWay {

	private final List<OSMMember> members;

	public OSMWayImpl(long id, int version, boolean visible, OSHDBTimestamp timestamp, long changeset, int userId,
			List<OSHDBTag> tags, List<OSMMember> members) {
		super(id, version, visible, timestamp, changeset, userId, tags);
		this.members = members;
	}

	public Iterable<OSMMember> getMembers() {
		return members;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((members == null) ? 0 : members.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof OSMWayImpl))
			return false;
		OSMWayImpl other = (OSMWayImpl) obj;
		if (members == null) {
			if (other.members != null)
				return false;
		} else if (!members.equals(other.members))
			return false;
		return true;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		return "OSMWayImpl ["+super.toString()+", members=" + (members != null ? members.subList(0, Math.min(members.size(), maxLen)) : null)+ "]";
	}
	
	

}
