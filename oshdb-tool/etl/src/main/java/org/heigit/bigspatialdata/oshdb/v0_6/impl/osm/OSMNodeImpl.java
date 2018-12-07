package org.heigit.bigspatialdata.oshdb.v0_6.impl.osm;

import java.util.List;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;

public class OSMNodeImpl extends OSMEntityImpl implements OSMNode {
	
	private final long longitude;
	private final long latitude;
	
	public OSMNodeImpl(long id, int version, boolean visible, OSHDBTimestamp timestamp, long changeset, int userId,
			List<OSHDBTag> tags, long longitude, long latitude) {
		super(id, version, visible, timestamp, changeset, userId, tags);
		this.longitude = longitude;
		this.latitude = latitude;
	}

	@Override
	public long getLongitude() {
		return longitude;
	}

	@Override
	public long getLatatitde() {
		return latitude;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (latitude ^ (latitude >>> 32));
		result = prime * result + (int) (longitude ^ (longitude >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof OSMNodeImpl))
			return false;
		OSMNodeImpl other = (OSMNodeImpl) obj;
		if (latitude != other.latitude)
			return false;
		if (longitude != other.longitude)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "OSMNodeImpl ["+super.toString()+", longitude=" + longitude + ", latitude=" + latitude + "]";
	}
	
		
	
}
