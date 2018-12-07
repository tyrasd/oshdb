package org.heigit.bigspatialdata.oshdb.v0_6.osm;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSMRelation extends OSMEntity {

	@Override
	default OSMType getType() {
		return OSMType.RELATION;
	}


}
