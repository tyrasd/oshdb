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
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osm.OSMWayImpl;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMWay;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class WayVersionsReader extends EntityVersionsReader implements Iterator<OSMWay> {

	private ObjectArrayList<OSMMember> members = new ObjectArrayList<>();
	private ObjectArrayList<OSMMember> prevMembers = new ObjectArrayList<>();

	private LongFunction<OSMMember> idxToMember;

	private OSMWay next = null;

	public Iterator<OSMWay> set(long id, ByteBuffer versions, boolean single, boolean visible, IntArrayList kvs,
			long timestamp, LongToIntFunction idxToUid, LongFunction<OSHDBTag> idxToTag,
			LongFunction<OSMMember> idxToMember) throws IOException {
		super.reset(id, versions, single, visible, kvs, timestamp, idxToUid, idxToTag);

		members.clear();
		prevMembers.clear();
		if (visible) {
			final int size = (int) SerializationUtils.readVulong(in);
			members.ensureCapacity(size);
			if (size > 4) {
				intReader.read(in, r -> {
					for (int i = 0; i < size; i++) {
						final long idx = r.next();
						final OSMMember member = idxToMember.apply(idx);
						members.add(member);
					}
				});
			}else {
				long idx = 0;
				for (int i = 0; i < size; i++) {
					idx = SerializationUtils.readVulong(in) + idx;
					final OSMMember member = idxToMember.apply(idx);
					members.add(member);
				}
			}
		}

		this.idxToMember = idxToMember;

		next = new OSMWayImpl(id, version, visible, new OSHDBTimestamp(timestamp), changeset, uid, tags, members);

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
	public OSMWay next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		OSMWay ret = next;
		next = null;
		return ret;
	}

	private OSMWay getNext() throws IOException {
		if (last)
			return null;

		if(!nextEntity()){
			return null;
		}

		if (visible) {
			if ((header & OSHDB.OSM_HEADER_CHG_EXT) != 0) {
				swapMembers();
				readLCSValue(in, prevMembers, members, idxToMember);
				prevMembers.clear();
			}
		}

		return new OSMWayImpl(id, version, visible, new OSHDBTimestamp(timestamp), changeset, uid, tags, members);
	}

	private void swapMembers() {
		ObjectArrayList<OSMMember> swap = prevMembers;
		prevMembers = members;
		members = swap;
	}

}
