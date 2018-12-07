package org.heigit.bigspatialdata.oshdb.v0_6.osm;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSMWay extends OSMEntity {
	
	@Override
	default OSMType getType() {
		return OSMType.WAY;
	}
	

}
