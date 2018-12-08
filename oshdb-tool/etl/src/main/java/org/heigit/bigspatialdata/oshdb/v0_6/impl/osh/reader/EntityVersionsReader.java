package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.ByteBufferBackedInputStream;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerReader;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class EntityVersionsReader {

	protected final ByteBufferBackedInputStream in = new ByteBufferBackedInputStream();
	protected final DeltaIntegerReader intReader = new DeltaIntegerReader("", false);

	protected ByteBuffer bytes;
	protected long id;

	protected int header;

	protected boolean last;
	protected boolean visible;

	protected int version;
	protected long timestamp;
	protected int uid;
	protected long changeset;

	protected ObjectArrayList<OSHDBTag> tags = new ObjectArrayList<>();
	protected ObjectArrayList<OSHDBTag> prevTags = new ObjectArrayList<>();

	protected LongToIntFunction idxToUid;
	protected LongFunction<OSHDBTag> idxToTag;

	protected void reset(long id, ByteBuffer bytes, boolean single, boolean visible, IntArrayList kvs, long timestamp,
			LongToIntFunction idxToUid, LongFunction<OSHDBTag> idxToTag) throws IOException {
		this.bytes = bytes;
		this.in.set(bytes);

		this.id = id;

		this.header = 0;
		this.last = single;
		this.visible = visible;

		if (!single) {
			version = (int) SerializationUtils.readVulong(in);
		} else {
			version = 1;
		}

		this.timestamp = timestamp;
		

		if (single) {
			uid = (int) SerializationUtils.readVulong(in);
		} else {
			final long idx = SerializationUtils.readVulong(in);
			uid = idxToUid.applyAsInt(idx);
		}
		
		changeset = SerializationUtils.readVulong(in);
		tags.clear();
		prevTags.clear();
		if (kvs.size() > 0 && visible) {
			if (single) {
				int size = kvs.size() / 2;
				for (long idx = 0; idx < size; idx++) {
					final OSHDBTag tag = idxToTag.apply(idx);
					tags.add(tag);
				}
			} else {
				int size = (int) SerializationUtils.readVulong(in);
				tags.ensureCapacity(size);
				if (size > 4) {
					intReader.read(in, r -> {
						for (int i = 0; i < size; i++) {
							final long idx = r.next();
							final OSHDBTag tag = idxToTag.apply(idx);
							tags.add(tag);
						}
					});
				} else {
					long idx = 0;
					for (int i = 0; i < size; i++) {
						idx = SerializationUtils.readVulong(in) + idx;
						final OSHDBTag tag = idxToTag.apply(idx);
						tags.add(tag);
					}
				}
			}
		}

		this.idxToUid = idxToUid;
		this.idxToTag = idxToTag;
	}

	protected boolean nextEntity() throws IOException {
		header = in.read();
		version += OSHDB.sortOrder.dir;

		last = (header & OSHDB.OSM_HEADER_END) != 0;
		while ((header & OSHDB.OSM_HEADER_MISSING) != 0) {
			if(last)
				return false;
			
			header = in.read();
			version += OSHDB.sortOrder.dir;
		}

		visible = (header & OSHDB.OSM_HEADER_VISIBLE) != 0;

		long dTimestamp = SerializationUtils.readVslong(in);
		timestamp = dTimestamp + timestamp;

		if ((header & OSHDB.OSM_HEADER_CHG_UID) != 0) {
			final long idx = SerializationUtils.readVulong(in);
			uid = idxToUid.applyAsInt(idx);
		}

		if ((header & OSHDB.OSM_HEADER_CHG_CS) != 0) {
			long dChangeset = SerializationUtils.readVslong(in);
			changeset = dChangeset + changeset;
		}

		if (visible) {
			if ((header & OSHDB.OSM_HEADER_CHG_TAGS) != 0) {
				swapTags();
				readLCSValue(in, prevTags, tags, idxToTag);
				prevTags.clear();
			}
		}
		return true;
	}

	private void swapTags() {
		ObjectArrayList<OSHDBTag> swap = prevTags;
		prevTags = tags;
		tags = swap;
	}

	protected <T extends Comparable<T>> void readLCSValue(InputStream in, ObjectArrayList<T> src,
			ObjectArrayList<T> dest, LongFunction<T> fromIdx) throws IOException {
		assert (dest.size() == 0);

		final int size = (int) SerializationUtils.readVulong(in);
		if (size == 0) {
			return;
		}
		dest.ensureCapacity(size);
		if (src.isEmpty()) {
			if (size > 4) {
				intReader.read(in, r -> {
					for (int i = 0; i < size; i++) {
						int idx = (int) r.next();
						T t = fromIdx.apply(idx);
						dest.add(t);
					}
				});
			} else {
				long idx = 0;
				for (int i = 0; i < size; i++) {
					idx = SerializationUtils.readVulong(in) + idx;
					T t = fromIdx.apply(idx);
					dest.add(t);
				}
			}
		} else {
			int srcPos = 0;
			int dstPos = 0;
			while (dstPos < size) {
				int action = in.read();
				int length = action & OSHDB.ACTION_LENGTH_BITMASK;
				if (length == OSHDB.ACTION_LENGTH_BITMASK) {
					length += SerializationUtils.readVulong(in);
				}

				switch (action & ~OSHDB.ACTION_LENGTH_BITMASK) {
				case OSHDB.ACTION_TAKE:
					for (int i = 0; i < length; i++) {
						final T t = src.get(srcPos++);
						dest.add(t);
					}
					dstPos += length;
					break;
				case OSHDB.ACTION_SKIP:
					srcPos += length;
					break;
				case OSHDB.ACTION_UPDATE:
					srcPos += length;
					// fall through
				case OSHDB.ACTION_ADD:
					final long addLength = length;
					if (addLength > 4) {
						intReader.read(in, r -> {
							for (int i = 0; i < addLength; i++) {
								final long idx = r.next();
								final T t = fromIdx.apply(idx);
								dest.add(t);
							}
						});
					} else {
						long idx = 0;
						for (int i = 0; i < addLength; i++) {
							idx = SerializationUtils.readVulong(in) + idx;
							final T t = fromIdx.apply(idx);
							dest.add(t);
						}
					}

					dstPos += length;
					break;
				}
			}
		}
	}

	protected static final byte[] EMPTY = new byte[0];
	protected static final int[] EMPTY_KEY_VALUE_ARRAY = new int[0];
	protected static final OSHDBTag[] EMPTY_TAG_ARRAY = new OSHDBTag[0];

	protected int[] toIntArray(List<OSHDBTag> tags) {
		int[] array = new int[tags.size() * 2];
		int i = 0;
		for (OSHDBTag tag : tags) {
			array[i++] = tag.getKey();
			array[i++] = tag.getValue();
		}
		return array;
	}

}
