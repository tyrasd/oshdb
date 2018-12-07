package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.ByteBufferBackedInputStream;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerReader;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

public abstract class OSHEntityReader implements OSHEntity {

	protected final ByteBufferBackedInputStream in = new ByteBufferBackedInputStream();
	protected final DeltaIntegerReader intReader = new DeltaIntegerReader("v6main", false);

	protected ByteBuffer bytes;

	protected long id;

	protected boolean single;
	protected boolean visible;
	protected boolean invalid;

	protected long minTimestamp, minLongitude, minLatitude;
	protected long maxTimestamp, maxLongitude, maxLatitude;

	protected final IntArrayList kvs = new IntArrayList();
	protected final IntArrayList uids = new IntArrayList();

	protected final LongToIntFunction uidLookup = u -> uids.getInt((int) u);
	protected final LongFunction<OSHDBTag> tagLookup = t -> {
		int idx = (int) t * 2;
		return new OSHDBTag(kvs.elements()[idx], kvs.elements()[idx + 1]);
	};

	protected void readCommonEnvelope(int header) throws IOException {
		minTimestamp = SerializationUtils.readVulong(in);
		
		
		uids.clear();
		if ((header & OSHDB.OSH_HEADER_SINGLE) == 0) {
			maxTimestamp = SerializationUtils.readVulong(in) + minTimestamp;
			
			final int size = (int) SerializationUtils.readVulong(in);
			uids.ensureCapacity(size);
			if(size > 4){
			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					uids.add((int) r.next());
				}
			});
			}else{
				int uid = 0;
				for(int i=0; i< size; i++){
					uid = (int) SerializationUtils.readVulong(in) + uid;
					uids.add(uid);
				}
			}

		} else {
			maxTimestamp = minTimestamp;
		}

		
		
		kvs.clear();
		if ((header & OSHDB.OSH_HEADER_HAS_TAGS) != 0) {
			final int size = (int) SerializationUtils.readVulong(in);
			kvs.ensureCapacity(size * 2);
			kvs.size(size * 2);
			if (size > 4) {
				intReader.read(in, r -> {
					int idx = 0;
					for (int i = 0; i < size; i++) {
						final int k = (int) r.next();
						kvs.elements()[idx] = k;
						idx += 2;
					}
				});

				intReader.read(in, r -> {
					int idx = 1;
					for (int i = 0; i < size; i++) {
						final int v = (int) r.next();
						kvs.elements()[idx] = v;
						idx += 2;
					}
				});
			} else {
				int idx = 0;
				int k = 0;
				for (int i = 0; i < size; i++) {
					k = (int) SerializationUtils.readVulong(in) + k;
					kvs.elements()[idx] = k;
					idx += 2;
				}
				idx = 1;
				int v = 0;
				for (int i = 0; i < size; i++) {
					v = (int) SerializationUtils.readVulong(in) + v;
					kvs.elements()[idx] = v;
					idx += 2;
				}
			}
		}
	}

	protected void readBackRef(LongArrayList backRefs) throws IOException {
		final int size = (int) SerializationUtils.readVulong(in);
		backRefs.ensureCapacity(size);
		if (size > 4) {
			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					long id = r.next();
					backRefs.add(id);
				}
			});
		} else {
			long id = 0;
			for (int i = 0; i < size; i++) {
				id = SerializationUtils.readVulong(in) + id;
				backRefs.add(id);
			}
		}
	}

	public long getMinLongitude() {
		return minLongitude;
	}

	public long getMinLatitude() {
		return minLatitude;
	}

	public long getMaxLongitude() {
		return maxLongitude;
	}

	public long getMaxLatitude() {
		return maxLatitude;
	}
}
