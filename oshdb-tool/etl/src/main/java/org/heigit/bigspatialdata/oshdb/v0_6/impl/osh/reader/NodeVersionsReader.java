package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osm.OSMNodeImpl;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class NodeVersionsReader extends EntityVersionsReader implements Iterator<OSMNode> {

	private OSMNode next = null;
	
	private long longitude, latitude;
	
	public Iterator<OSMNode> set(long id, ByteBuffer versions,boolean single, boolean visible, IntArrayList kvs, boolean point, long timestamp, long longitude, long latitude ,LongToIntFunction idxToUid, LongFunction<OSHDBTag> idxToTag) throws IOException {
		super.reset(id, versions, single, visible, kvs, timestamp, idxToUid, idxToTag);
		
		if(!single && visible && !point){
				long dLongitude = SerializationUtils.readVslong(in);
				long dLatitude = SerializationUtils.readVslong(in);
				this.longitude = dLongitude + longitude;
				this.latitude = dLatitude + latitude;
		}else {
			this.longitude = longitude;
			this.latitude = latitude;
		}
		
		
		next = new OSMNodeImpl(id, version, visible, new OSHDBTimestamp(timestamp), changeset, uid, tags, this.longitude, this.latitude);
		
		return this;
	}
	
	@Override
	public boolean hasNext() {
		try {
			return next != null || (next = getNext()) != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public OSMNode next() {
		if(!hasNext()){
			throw new NoSuchElementException();
		}
	
		OSMNode ret = next;
		next = null;
		
		return ret;
	}
	
	private OSMNode getNext() throws IOException {
		if(last)
			return null;
		
		if(!nextEntity())
			return null;
		
		if(visible){
			if((header & OSHDB.OSM_HEADER_CHG_EXT) != 0){
				final long dLongitude = SerializationUtils.readVslong(in);
				final long dLatitude = SerializationUtils.readVslong(in);
				
				longitude = dLongitude + longitude;
				latitude = dLatitude + latitude;
			}
		}
		
		return new OSMNodeImpl(id, version, visible, new OSHDBTimestamp(timestamp), changeset, uid, tags, longitude, latitude);
	}	

}
