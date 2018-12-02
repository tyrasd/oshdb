package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class OSHNodeBuilder extends OSHBuilder {

	private final FastByteArrayOutputStream aux = new FastByteArrayOutputStream(4096);
	
	private long prevLongitude = 0;
	private long prevLatitude = 0;
		
	public void build(OutputStream out, List<OSMNode> versions, 
			long baseTimestamp,
			long baseLongitude, long baseLatitude,
			LongToIntFunction uidToIdx,
			ToIntFunction<OSHDBTag> tagToIdx) throws IOException {
		
		super.reset();

		prevLongitude = prevLatitude = 0;
		
		final Iterator<OSMNode> itr = versions.iterator();
		while (itr.hasNext()) {
			aux.reset();
			final OSMNode version = itr.next();
			
			int header = buildEntity(aux, version, baseTimestamp, uidToIdx, tagToIdx) ;
			
			if(version.isVisible()){
				final long longitude = version.getLon() - baseLongitude;
				final long latitude = version.getLat() - baseLatitude;
				
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

			out.write(header);
			out.write(aux.array, 0, aux.length);	
		}
	}
}
