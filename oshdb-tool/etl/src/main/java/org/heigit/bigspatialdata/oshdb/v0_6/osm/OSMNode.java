package org.heigit.bigspatialdata.oshdb.v0_6.osm;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSMNode extends OSMEntity {

	long getLongitude();

	long getLatatitde();

	@Override
	default OSMType getType() {
		return OSMType.NODE;
	}
}
