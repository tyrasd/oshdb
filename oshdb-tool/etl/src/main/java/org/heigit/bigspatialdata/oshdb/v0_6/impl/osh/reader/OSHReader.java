package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerReader;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

public abstract class OSHReader<E extends OSMEntity> {
	protected static final byte[] EMPTY = new byte[0];
	protected static final int[] EMPTY_KEY_VALUE_ARRAY = new int[0];
	protected static final OSHDBTag[] EMPTY_TAG_ARRAY = new OSHDBTag[0];

	protected final DeltaIntegerReader intReader = new DeltaIntegerReader("OSHReader", false);
	protected InputStream in;

	protected int header;
	protected boolean isSingle;

	protected long id;
	protected int version;
	protected boolean isVisible;
	protected boolean isLast = false;
	protected long timestamp;
	protected int uid;
	protected long changeset;
	protected List<OSHDBTag> tags = Lists.newArrayList();
	protected List<OSHDBTag> auxTags = Lists.newArrayList();

	protected LongToIntFunction idxToUid;
	protected LongFunction<OSHDBTag> idxToTag;

//	protected void reset(ByteBuffer bytes, int header, long id, long baseTimestamp, List<OSHDBTag> tags,
//			LongToIntFunction idxToUid, LongFunction<OSHDBTag> idxToTag) {
//		this.in.array = bytes.array();
//		this.in.offset = bytes.position();
//		this.in.length = bytes.limit();
//		this.in.reset();
		
	protected void reset(InputStream in, int header, long id, long baseTimestamp, List<OSHDBTag> tags,
				LongToIntFunction idxToUid, LongFunction<OSHDBTag> idxToTag) {
		this.in = in;
		this.header = header;
		if (header != 0)
			isSingle = true;
		else
			isSingle = false;

		this.id = id;
		this.version = (OSHDB.sortOrder == OSHDB.SortOrder.ASC) ? 0 : 2;
		this.isLast = false;
		this.timestamp = baseTimestamp;
		this.uid = 0;
		this.changeset = 0;

		this.tags.clear();
		if (tags != null) {
			this.tags.addAll(tags);
		}

		this.auxTags.clear();

		this.idxToUid = idxToUid;
		this.idxToTag = idxToTag;
	}

	public boolean hasNext() {
		return !isLast;
	}

	public E read() throws IOException {
		if (isSingle) {
			return readSingle();
		}
		return readMulti();
	}

	private E readSingle() throws IOException {
		if ((header & OSHDB.OSM_HEADER_COMPLETE) != 0) {
			version += OSHDB.sortOrder.dir;
		} else {
			version = (int) SerializationUtils.readVulong(in);
		}

		uid = (int) SerializationUtils.readVulong(in);
		changeset = SerializationUtils.readVulong(in);
		isVisible = (header & OSHDB.OSM_HEADER_VISIBLE) != 0;
		isLast = true;

		E e = readExt(in, false);
		return e;
	}

	private E readMulti() throws IOException {
		header = in.read();

		if ((header & OSHDB.OSM_HEADER_COMPLETE) != 0) {
			version += OSHDB.sortOrder.dir;
		} else {
			version = (int) SerializationUtils.readVulong(in);
		}

		timestamp += SerializationUtils.readVslong(in);

		if ((header & OSHDB.OSM_HEADER_CHG_UID) != 0) {
			long idx = SerializationUtils.readVulong(in);
			uid = idxToUid.applyAsInt(idx);
		}

		if ((header & OSHDB.OSM_HEADER_CHG_CS) != 0) {
			changeset += SerializationUtils.readVslong(in);
		}
		isVisible = (header & OSHDB.OSM_HEADER_VISIBLE) != 0;
		isLast = (header & OSHDB.OSM_HEADER_END) != 0;

		if (isVisible) {
			if ((header & OSHDB.OSM_HEADER_CHG_TAGS) != 0) {
				final List<OSHDBTag> newTags = readLCSValue(in, tags, auxTags, idxToTag);
				auxTags = tags;
				tags = newTags;
			}
		}

		E e = readExt(in, isVisible && (header & OSHDB.OSM_HEADER_CHG_EXT) != 0);
		return e;
	}

	protected abstract E readExt(InputStream in, boolean changed) throws IOException;

	protected <T extends Comparable<T>> List<T> readLCSValue(InputStream in, List<T> src, List<T> dest,
			LongFunction<T> fromIdx) throws IOException {
		assert (dest.size() == 0);

		final int size = (int) SerializationUtils.readVulong(in);
		if (size == 0) {
			src.clear();
			return dest;
		}

		if (src.isEmpty()) {
			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					int idx = (int) r.next();
					T t = fromIdx.apply(idx);
					dest.add(t);
				}
			});
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
					intReader.read(in, r -> {
						for (int i = 0; i < addLength; i++) {
							final long idx = r.next();
							final T t = fromIdx.apply(idx);
							dest.add(t);
						}
					});

					dstPos += length;
					break;
				}
			}
		}
		src.clear();
		return dest;
	}

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
