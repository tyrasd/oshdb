package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSHNodeReader extends OSHReader<OSMNode> {

	private long longitude = 0;
	private long latitude = 0;
	
	public void reset(InputStream in,int header, long id, long baseTimestamp, List<OSHDBTag> tags, long baseLongitude, long baseLatitude, LongToIntFunction idxToUid,
			LongFunction<OSHDBTag> idxToTag) {
		reset(in,header, id, baseTimestamp, tags, idxToUid, idxToTag);
		longitude = baseLongitude;
		latitude = baseLatitude;
	}
	
	@Override
	protected OSMNode readExt(InputStream in, boolean changed) throws IOException {
		if(changed){
			longitude = SerializationUtils.readVslong(in) + longitude;
			latitude = SerializationUtils.readVslong(in) + latitude;
		}
		return new OSMNode(id, version*((isVisible)?1:-1), new OSHDBTimestamp(timestamp), changeset, uid, ((isVisible)?toIntArray(tags):EMPTY_KEY_VALUE_ARRAY), longitude, latitude);
	}
}
