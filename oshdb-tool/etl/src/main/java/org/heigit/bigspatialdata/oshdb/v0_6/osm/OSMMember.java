package org.heigit.bigspatialdata.oshdb.v0_6.osm;

import java.util.function.IntFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.v0_6.osh.OSHEntity;

public class OSMMember implements Comparable<OSMMember> {
	private final long id;
	private final OSMType type;
	private final int roleId;
	private OSHEntity entity;
	
	
	public static OSMMember node(final long id){
		return new OSMMember(id, OSMType.NODE, -1);
	}
	
	public static OSMMember node(final long id, final int roleId){
		return new OSMMember(id, OSMType.NODE, roleId);
	}
	
	public static OSMMember way(final long id, final int roleId){
		return new OSMMember(id, OSMType.WAY, roleId);
	}
	
	public static OSMMember relation(final long id, final int roleId){
		return new OSMMember(id, OSMType.RELATION, roleId);
	}
	
	public OSMMember(final long id, final OSMType type, final int roleId) {
		this(id, type, roleId, null);
	}
	
	public OSMMember(final long id, final OSMType type, final int roleId, OSHEntity entity) {
		this.id = id;
		this.type = type;
		this.roleId = roleId;
		this.entity = entity;
	}
	
	public long getId() {
		return id;
	}

	public OSMType getType() {
		return type;
	}

	public int getRoleId() {
		return roleId;
	}

	public OSHDBRole getRole(IntFunction<OSHDBRole> resolve) {
		return resolve.apply(roleId);
	}
	
	public OSHEntity getEntity() {
		return entity;
	}
	
	public void setEntity(OSHEntity entity) {
		this.entity = entity;
	}
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + roleId;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof OSMMember))
			return false;
		OSMMember other = (OSMMember) obj;
		if (id != other.id)
			return false;
		if (roleId != other.roleId)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	@Override
	public int compareTo(OSMMember o) {
		int c = Integer.compare(type.intValue(), o.type.intValue());
		if(c == 0){
			c = Long.compare(id, o.id);
			if(c == 0)
				c = Integer.compare(roleId, o.roleId);
		}
		return c;
	}

	@Override
	public String toString() {
		return "OSMMember [id=" + id + ", type=" + type + ", roleId=" + roleId + ", entity=" + entity + "]";
	}
	
	

}
