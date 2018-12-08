package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public abstract class OSHEntitySerializer {

	protected final SerializationUtils serUtil = new SerializationUtils();
	protected final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("", false);

	protected IntSortedSet uidSet = new IntAVLTreeSet();
	protected SortedSet<OSHDBTag> tagSet = new TreeSet<>();
	protected final Object2IntMap<OSHDBTag> tagToIdx = new Object2IntOpenHashMap<>();
	protected final Long2IntMap uidToIdx = new Long2IntOpenHashMap();

	protected final LongToIntFunction uidLookup = uidToIdx::get;
	protected final ToIntFunction<OSHDBTag> tagLookup = tagToIdx::getInt;

	protected int maxVersion = Integer.MIN_VALUE;
	protected long minTimestamp, maxTimestamp;
	protected boolean lastVisibility = false;
	
	public boolean hasTags(){
		return tagSet.size() > 0;
	}

	protected void reset() {
		uidSet.clear();
		tagSet.clear();
		tagToIdx.clear();
		uidToIdx.clear();

		maxVersion = Integer.MIN_VALUE;
		lastVisibility = false;

		minTimestamp = Long.MAX_VALUE;
		maxTimestamp = Long.MIN_VALUE;
	}

	protected void addEntity(OSMEntity osm) {
		final int version = osm.getVersion();
		final long timestamp = osm.getTimestamp().getRawUnixTimestamp();
		
		if (maxVersion < version) {
			maxVersion = version;
			lastVisibility = osm.isVisible();
			maxTimestamp = timestamp;
		}

		if (minTimestamp > timestamp) {
			minTimestamp = timestamp;
		}
		
		osm.getTags().forEach(tagSet::add);
		uidSet.add(osm.getUserId());
	}

	protected int commonHeader(List<? extends OSMEntity> versions) {
		int header = 0;
		if (maxVersion == 1) {
			header |= OSHDB.OSH_HEADER_SINGLE;
		}
		if (lastVisibility) {
			header |= OSHDB.OSH_HEADER_VISIBLE;
		}
		if (tagSet.size() > 0) {
			header |= OSHDB.OSH_HEADER_HAS_TAGS;
		}
		return header;
	}

	protected void writeCommonOSHEnvelope(OutputStream out, int header) throws IOException {
		serUtil.writeVulong(out, minTimestamp);
		

		if ((header & OSHDB.OSH_HEADER_SINGLE) == 0) {
			final long dTimestamp = maxTimestamp - minTimestamp;
			serUtil.writeVulong(out, dTimestamp);
			
			
			serUtil.writeVulong(out, uidSet.size());
			if (uidSet.size() > 4) {
				intWriter.write(out, writer -> {
					int idx = 0;
					for (int uid : uidSet) {
						writer.write(uid);
						uidToIdx.put(uid, idx++);
					}
				});
			} else {
				int idx = 0;
				int lastUid = 0;
				for (int uid : uidSet) {
					serUtil.writeVulong(out, uid - lastUid);
					uidToIdx.put(uid, idx++);
					lastUid = uid;
				}
			}
		}
		
		if ((header & OSHDB.OSH_HEADER_HAS_TAGS) != 0) {
			serUtil.writeVulong(out, tagSet.size());
			if (tagSet.size() > 4) {
				intWriter.write(out, writer -> {
					int idx = 0;
					for (OSHDBTag tag : tagSet) {
						writer.write(tag.getKey());
						tagToIdx.put(tag, idx++);
					}
				});
				intWriter.write(out, writer -> {
					for (OSHDBTag tag : tagSet) {
						writer.write(tag.getValue());
					}
				});
			}else {
				int idx = 0;
				int lastKey = 0;
				for(OSHDBTag tag: tagSet){
					serUtil.writeVulong(out, tag.getKey() - lastKey);
					lastKey = tag.getKey();
					tagToIdx.put(tag, idx++);
				}
				int lastValue = 0;
				for(OSHDBTag tag: tagSet){
					serUtil.writeVulong(out, tag.getValue() - lastValue);
					lastValue = tag.getValue();
				}
			}
			
		}
	}

	protected void writeBackRef(OutputStream out, long[] backRef) throws IOException {
		serUtil.writeVulong(out, backRef.length);
		if (backRef.length > 4) {
			intWriter.write(out, writer -> {
				for (long r : backRef) {
					writer.write(r);
				}
			});
		} else {
			long lastR = 0;
			for (long r : backRef) {
				serUtil.writeVulong(out, r - lastR);
				lastR = r;
			}
		}
	}

}
