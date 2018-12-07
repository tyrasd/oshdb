package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.LongFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader.WayVersionsReader;
import org.heigit.bigspatialdata.oshdb.v0_6.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMWay;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class OSHWayReader extends OSHEntityReader implements OSHWay {

	private final LongArrayList backRefRelations = new LongArrayList();
	
	//private final LongArrayList members = new LongArrayList();
	private final ObjectArrayList<OSMMember> members = new ObjectArrayList<>();
	private final LongFunction<OSMMember> memberLookup = idx -> members.get((int) idx);

	private ByteBuffer versions;
	private WayVersionsReader versionReader = new WayVersionsReader();

	public OSHWayReader read(long id, ByteBuffer bytes) throws IOException {
		this.bytes = bytes;
		this.in.set(bytes);

		this.id = id;
		int header = in.read();

		single = (header & OSHDB.OSH_HEADER_SINGLE) != 0;
		visible = (header & OSHDB.OSH_HEADER_VISIBLE) != 0;
		invalid = (header & OSHDB.OSH_HEADER_INVALID) != 0;

		if (!invalid) {
			minLongitude = SerializationUtils.readVslong(in);
			minLatitude = SerializationUtils.readVslong(in);
			maxLongitude = SerializationUtils.readVulong(in) + minLongitude;
			maxLatitude = SerializationUtils.readVulong(in) + minLatitude;
		}

		int size = (int) SerializationUtils.readVulong(in);

		members.clear();
		if (size > 0) {
			members.ensureCapacity(size);
			if (size > 4) {
				intReader.read(in, r -> {
					for (int i = 0; i < size; i++) {
						long mId = r.next();
						members.add(new OSMMember(mId, OSMType.NODE, -1));
					}
				});
			} else {
				long mId = 0;
				for (int i = 0; i < size; i++) {
					mId = SerializationUtils.readVulong(in) + mId;
					members.add(new OSMMember(mId, OSMType.NODE, -1));
				}
			}
		}

		versions = bytes.slice();
		return this;
	}

	public Iterator<OSMWay> iterator() throws IOException {
		versions.rewind();
		return versionReader.set(id, versions, single, visible, kvs, maxTimestamp, uidLookup, tagLookup, memberLookup);
	}
}
