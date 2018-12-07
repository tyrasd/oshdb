package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader.NodeVersionsReader;
import org.heigit.bigspatialdata.oshdb.v0_6.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class OSHNodeReader extends OSHEntityReader implements OSHNode {
	
	private ByteBuffer versions;
	private NodeVersionsReader versionReader = new NodeVersionsReader();
	
	private final LongArrayList backRefWays = new LongArrayList();
	private final LongArrayList backRefRelations = new LongArrayList();
	
	private boolean point;
		
	public OSHNodeReader read(long id, ByteBuffer bytes) throws IOException{
		this.bytes = bytes;
		this.in.set(bytes);
		
		this.id = id;
		int header = in.read();
		
		single = (header & OSHDB.OSH_HEADER_SINGLE) != 0;
		visible = (header & OSHDB.OSH_HEADER_VISIBLE) != 0;
		invalid = (header & OSHDB.OSH_HEADER_INVALID) != 0;
		point = (header & OSHDB.OSH_HEADER_NODE_POINT) != 0;
		
		if (!invalid) {
			minLongitude = SerializationUtils.readVslong(in);
			minLatitude = SerializationUtils.readVslong(in);
			if(!single && !point ) {
				maxLongitude = SerializationUtils.readVulong(in) + minLongitude;
				maxLatitude = SerializationUtils.readVulong(in) + minLatitude;
			} else {
				maxLongitude = minLongitude;
				maxLatitude = minLatitude;
			}
		} else {
			minLongitude = minLatitude = Long.MAX_VALUE;
			maxLongitude = maxLatitude = Long.MIN_VALUE;
		}
		

		
		readCommonEnvelope(header);
		
		backRefWays.clear();
		if((header & OSHDB.OSH_HEADER_NODE_BACKREF_WAY) != 0){
			readBackRef(backRefWays);
		}
		backRefRelations.clear();
		if((header & OSHDB.OSH_HEADER_NODE_BACKREF_REL) != 0){
			readBackRef(backRefRelations);
		}
		

		
		versions = bytes.slice();
		return this;
	}

	public Iterator<OSMNode> iterator() throws IOException{
		versions.rewind();
		return versionReader.set(id, versions, single, visible, kvs, point, maxTimestamp,minLongitude,minLatitude, uidLookup, tagLookup);
	}
}
