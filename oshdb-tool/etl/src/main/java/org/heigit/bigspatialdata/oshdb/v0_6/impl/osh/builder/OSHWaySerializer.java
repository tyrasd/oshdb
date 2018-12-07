package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.ToIntFunction;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.OSHNodeReader;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMWay;

import com.google.common.cache.LoadingCache;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class OSHWaySerializer extends OSHEntitySerializer {

	private final WayVersionsSerializer serializer = new WayVersionsSerializer();

	private SortedSet<OSMMember> memberSet = new TreeSet<>();

	protected final Object2IntMap<OSMMember> memberToIdx = new Object2IntOpenHashMap<>();
	protected final ToIntFunction<OSHDBTag> memberLookup = memberToIdx::getInt;

	public void reset() {
		super.reset();
		memberSet.clear();
	}

	public void addWay(OSMWay osm) {
		addEntity(osm);

		if (osm.isVisible()) {
			for (OSMMember m : osm.getMembers()) {
				memberSet.add(m);
			}
		}
	}

	public void serialize(OutputStream out, List<OSMWay> versions, LoadingCache<Long, ByteBuffer> nodeCache)
			throws IOException {
		int header = commonHeader(versions);

		long minLongitude, minLatitude, maxLongitude, maxLatitude;
		minLongitude = minLatitude = Long.MAX_VALUE;
		maxLongitude = maxLatitude = Long.MIN_VALUE;

		OSHNodeReader oshNode = new OSHNodeReader();
		for (OSMMember m : memberSet) {
			long mId = m.getId();
			ByteBuffer bytes;
			try {
				bytes = nodeCache.get(mId);
			} catch (ExecutionException e) {
				throw new IOException(e);
			}
			oshNode.read(mId, bytes);

			minLongitude = Math.min(minLongitude, oshNode.getMinLongitude());
			minLatitude = Math.min(minLatitude, oshNode.getMinLatitude());
			maxLongitude = Math.max(maxLongitude, oshNode.getMaxLongitude());
			maxLatitude = Math.max(maxLatitude, oshNode.getMaxLatitude());
		}

		if (minLongitude == Long.MAX_VALUE || minLatitude == Long.MAX_VALUE) {
			header |= OSHDB.OSH_HEADER_INVALID;
		}

		out.write(header);

		if ((header & OSHDB.OSH_HEADER_INVALID) == 0) {
			serUtil.writeVslong(out, minLongitude);
			serUtil.writeVslong(out, minLatitude);
			final long dLongitude = maxLongitude - minLongitude;
			final long dLatitude = maxLatitude - minLatitude;
			serUtil.writeVulong(out, dLongitude);
			serUtil.writeVulong(out, dLatitude);
		}

		writeCommonOSHEnvelope(out, header);

		serUtil.writeVulong(out, memberSet.size());
		if (memberSet.size() > 4) {
			intWriter.write(out, writer -> {
				int idx = 0;
				for (OSMMember m : memberSet) {
					writer.write(m.getId());
					memberToIdx.put(m, idx++);
				}
			});
		} else {
			long lastMId = 0;
			int idx = 0;
			for (OSMMember m : memberSet) {
				serUtil.writeVulong(out, m.getId() - lastMId);
				memberToIdx.put(m, idx++);
				lastMId = m.getId();
			}
		}

		boolean visible = (header & OSHDB.OSH_HEADER_VISIBLE) != 0;
		boolean single = (header & OSHDB.OSH_HEADER_SINGLE) != 0;
		boolean hasTags = (header & OSHDB.OSH_HEADER_HAS_TAGS) != 0;

		serializer.serialize(out, versions, visible, single, hasTags, uidToIdx, tagToIdx, memberToIdx);
	}

}
