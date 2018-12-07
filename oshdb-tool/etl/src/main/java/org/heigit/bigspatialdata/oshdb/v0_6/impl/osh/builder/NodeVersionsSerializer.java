package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class NodeVersionsSerializer extends EntityVersionsSerializer {
	
	private final FastByteArrayOutputStream aux = new FastByteArrayOutputStream(4096);
	
	private long prevLongitude, prevLatitude;

	public void serialize(OutputStream output, List<OSMNode> versions, boolean visible, boolean single, boolean hasTags, boolean point, long longitude, long latitude, LongToIntFunction uidToIdx,
			ToIntFunction<OSHDBTag> tagToIdx) throws IOException{
		Iterator<OSMNode> itr = versions.iterator();
		OSMNode version = itr.next();
		first(output, version, single, hasTags, uidToIdx, tagToIdx);
	
		if(!single && visible && !point){
			longitude = version.getLongitude() - longitude;
			latitude = version.getLatatitde() - latitude;
			serUtils.writeVslong(output, longitude);
			serUtils.writeVslong(output, latitude);
			prevLongitude = version.getLongitude();
			prevLatitude = version.getLatatitde();
		}else {
			prevLongitude = longitude;
			prevLatitude = latitude;
		}
		
		
		while(itr.hasNext()){
			aux.reset();
			version = itr.next();
			
			int header = 0;
			while(((header = serializeEntity(aux,version,uidToIdx,tagToIdx)) & OSHDB.OSM_HEADER_MISSING) != 0){
				output.write(header);
			}
			
					
			if(version.isVisible()){
				longitude = version.getLongitude();
				latitude = version.getLatatitde();
				
				if (longitude != prevLongitude || latitude != prevLatitude) {
					header |= OSHDB.OSM_HEADER_CHG_EXT;
					final long dLongitude = longitude - prevLongitude;
					serUtils.writeVslong(aux, dLongitude);
					final long dLatitude = latitude - prevLatitude;
					serUtils.writeVslong(aux, dLatitude);
				}
				prevLongitude = longitude;
				prevLatitude = latitude;
			}
			
			if(!itr.hasNext()) {
				header |= OSHDB.OSM_HEADER_END;
			}

			output.write(header);
			output.write(aux.array, 0, aux.length);
		}
	}
}
