package org.heigit.bigspatialdata.oshdb.v0_6.osm;

import java.util.Collections;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public interface OSMEntity {

	long getId();

	OSMType getType();

	int getVersion();

	OSHDBTimestamp getTimestamp();

	long getChangeset();

	int getUserId();

	boolean isVisible();
	
	Iterable<OSHDBTag> getTags();
	
	default Iterable<OSMMember> getMembers(){
	    return Collections.emptyList();
	}

}
