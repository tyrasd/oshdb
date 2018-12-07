package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

public class OSHNodeSerializer extends OSHEntitySerializer {
	
	private final NodeVersionsSerializer serialize = new NodeVersionsSerializer();
	
	private long minLongitude, maxLongitude;
	private long minLatitude, maxLatitude;
	
	
	
	public long getMinLongitude() {
		return minLongitude;
	}

	public long getMaxLongitude() {
		return maxLongitude;
	}

	public long getMinLatitude() {
		return minLatitude;
	}

	public long getMaxLatitude() {
		return maxLatitude;
	}

	public void reset() {
		super.reset();
		minLongitude = minLatitude = Long.MAX_VALUE;
		maxLongitude = maxLatitude = Long.MIN_VALUE;
	}
	
	public void addNode(OSMNode osm) {
		addEntity(osm);
		
		if (osm.isVisible()) {
			final long longitude = osm.getLongitude();
			final long latitude = osm.getLatatitde();

			if (minLongitude > longitude && longitude >= OSHDB.VALID_MIN_LONGITUDE) {
				minLongitude = longitude;
			}
			if (minLatitude > latitude && latitude >= OSHDB.VALID_MIN_LATITUDE) {
				minLatitude = latitude;
			}

			if (maxLongitude < longitude && longitude <= OSHDB.VALID_MAX_LONGITUDE) {
				maxLongitude = longitude;
			}
			if (maxLatitude < latitude && latitude <= OSHDB.VALID_MAX_LATITUDE) {
				maxLatitude = latitude;
			}
		}
	}
	
	public void serialize(OutputStream out, List<OSMNode> versions, long[] ways, long[] relations) throws IOException{
		int header = commonHeader(versions);
				
		if (minLongitude == maxLongitude && minLatitude == maxLatitude) {
			header |= OSHDB.OSH_HEADER_NODE_POINT;
		} else if (minLongitude == Long.MAX_VALUE || minLatitude == Long.MAX_VALUE) {
			header |= OSHDB.OSH_HEADER_INVALID;
		}
		
		if(ways.length > 0){
			header |= OSHDB.OSH_HEADER_NODE_BACKREF_WAY;
		}
		if(relations.length > 0){
			header |= OSHDB.OSH_HEADER_NODE_BACKREF_REL;
		}
			
		
		out.write(header);
		
		boolean visible = (header & OSHDB.OSH_HEADER_VISIBLE) != 0;
		boolean single = (header & OSHDB.OSH_HEADER_SINGLE) != 0;
		boolean hasTags = (header & OSHDB.OSH_HEADER_HAS_TAGS) != 0;
		boolean point = (header & OSHDB.OSH_HEADER_NODE_POINT) != 0;
		
		if ((header & OSHDB.OSH_HEADER_INVALID) == 0) {
			serUtil.writeVslong(out, minLongitude);
			serUtil.writeVslong(out, minLatitude);
			
			if (!single && !point) {
				final long dLongitude = maxLongitude - minLongitude;
				final long dLatitude = maxLatitude - minLatitude;
				serUtil.writeVulong(out, dLongitude);
				serUtil.writeVulong(out, dLatitude);
			}
		}


		
		writeCommonOSHEnvelope(out, header);
		
		if(ways.length > 0){
			writeBackRef(out, ways);
		}
		
		if(relations.length > 0){
			writeBackRef(out, relations);			
		}
		
		long longitude = minLongitude;
		long latitude = minLatitude;
		


		serialize.serialize(out, versions, visible, single, hasTags, point, longitude, latitude, uidToIdx, tagToIdx);
	}
}
