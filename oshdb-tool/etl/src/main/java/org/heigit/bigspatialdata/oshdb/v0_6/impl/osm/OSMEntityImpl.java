package org.heigit.bigspatialdata.oshdb.v0_6.impl.osm;

import java.util.List;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;

public abstract class OSMEntityImpl implements OSMEntity {

	protected final long id;

	protected final int version;
	protected final boolean visible;
	protected final OSHDBTimestamp timestamp;
	protected final long changeset;
	protected final int userId;
	
	protected final List<OSHDBTag> tags;
	
	
	
	protected OSMEntityImpl(long id, int version, boolean visible, OSHDBTimestamp timestamp, long changeset, int userId,
			List<OSHDBTag> tags) {
		this.id = id;
		this.version = version;
		this.visible = visible;
		this.timestamp = timestamp;
		this.changeset = changeset;
		this.userId = userId;
		this.tags = tags;
	}

	@Override
	public long getId() {
		return id;
	}
	
	@Override
	public int getVersion() {
		return version;
	}
	@Override
	public OSHDBTimestamp getTimestamp() {
		return timestamp;
	}
	@Override
	public long getChangeset() {
		return changeset;
	}
	@Override
	public int getUserId() {
		return userId;
	}
	@Override
	public boolean isVisible() {
		return visible;
	}
	@Override
	public Iterable<OSHDBTag> getTags() {
		return tags;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (changeset ^ (changeset >>> 32));
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((tags == null) ? 0 : tags.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + userId;
		result = prime * result + version;
		result = prime * result + (visible ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof OSMEntityImpl))
			return false;
		OSMEntityImpl other = (OSMEntityImpl) obj;
		if (changeset != other.changeset)
			return false;
		if (id != other.id)
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else if (!tags.equals(other.tags))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (userId != other.userId)
			return false;
		if (version != other.version)
			return false;
		if (visible != other.visible)
			return false;
		return true;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		return "id=" + id + ", version=" + version + ", visible=" + visible + ", timestamp=" + timestamp
				+ ", changeset=" + changeset + ", userId=" + userId + ", tags="
				+ (tags != null ? tags.subList(0, Math.min(tags.size(), maxLen)) : null);
	}
	
	
	
}
