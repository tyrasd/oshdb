package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS.Action;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS.LCSResult;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class EntityVersionsSerializer {
	protected final SerializationUtils serUtils = new SerializationUtils();
	protected final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("oshbuilder", false);
	protected final LCS lcs = new LCS();

	private int prevVersion = 0;
	private long prevTimestamp = 0;
	private int prevUid = 0;
	private long prevChangeset = 0;

	private ObjectArrayList<OSHDBTag> prevTags = new ObjectArrayList<>();
	private ObjectArrayList<OSHDBTag> tags = new ObjectArrayList<>();

	protected void first(OutputStream output, OSMEntity entity, boolean single, boolean hasTags,
			LongToIntFunction uidToIdx, ToIntFunction<OSHDBTag> tagToIdx) throws IOException {
		int version = entity.getVersion();

		if (!single) {
			serUtils.writeVulong(output, entity.getVersion());
		}
		prevVersion = version;

		prevTimestamp = entity.getTimestamp().getRawUnixTimestamp();

		
		
		int uid = entity.getUserId();
		if (single) {
			serUtils.writeVulong(output, uid);
		} else {
			int idx = uidToIdx.applyAsInt(uid);
			serUtils.writeVulong(output, idx);
		}
		prevUid = uid;
		

		long changeset = entity.getChangeset();
		serUtils.writeVulong(output, changeset);
		prevChangeset = changeset;

		tags.clear();
		if (!single && entity.isVisible() && hasTags) {
			addAll(tags, entity.getTags());
			serUtils.writeVulong(output, tags.size());
			if (tags.size() > 4) {
				intWriter.write(output, writer -> {
					for (OSHDBTag t : tags) {
						int idx = tagToIdx.applyAsInt(t);
						writer.write(idx);
					}
				});
			} else {
				int lastIdx = 0;
				for (OSHDBTag t : tags) {
					int idx = tagToIdx.applyAsInt(t);
					serUtils.writeVulong(output, idx - lastIdx);
					lastIdx = idx;
				}
			}
		}
		swapTagLists();
	}

	protected int serializeEntity(OutputStream output, OSMEntity entity, LongToIntFunction uidToIdx,
			ToIntFunction<OSHDBTag> tagToIdx) throws IOException {
		int header = 0;

		final int version = entity.getVersion();
		int dVersion = Math.abs(version - prevVersion);

		if (dVersion != 1) {
			prevVersion += OSHDB.sortOrder.dir;
			return OSHDB.OSM_HEADER_MISSING;
		}

		prevVersion = version;

		final long timestamp = entity.getTimestamp().getRawUnixTimestamp();
		final long dTimestamp = timestamp - prevTimestamp;
		serUtils.writeVslong(output, dTimestamp);
		prevTimestamp = timestamp;

		final int uid = entity.getUserId();
		if (uid != prevUid) {
			header |= OSHDB.OSM_HEADER_CHG_UID;
			int idx = uidToIdx.applyAsInt(uid);
			serUtils.writeVulong(output, idx);
		}
		prevUid = uid;

		final long changeset = entity.getChangeset();
		final long dChangeset = changeset - prevChangeset;
		if (dChangeset != 0) {
			header |= OSHDB.OSM_HEADER_CHG_CS;
			serUtils.writeVslong(output, dChangeset);
		}
		prevChangeset = changeset;

		if (entity.isVisible()) {
			header |= OSHDB.OSM_HEADER_VISIBLE;

			addAll(tags, entity.getTags());
			if (writeLCSValue(output, tags, prevTags, tagToIdx)) {
				header |= OSHDB.OSM_HEADER_CHG_TAGS;
			}
			swapTagLists();
		}
		return header;
	}

	protected <T extends Comparable<T>> boolean writeLCSValue(OutputStream out, List<T> curr, List<T> prev,
			ToIntFunction<T> toIdx) throws IOException {
		if (!curr.isEmpty()) {
			if (prev.isEmpty()) {
				serUtils.writeVulong(out, curr.size());
				if (curr.size() > 4) {
					intWriter.write(out, writer -> {
						for (T t : curr) {
							int idx = toIdx.applyAsInt(t);
							writer.write(idx);
						}
					});
				} else {
					int lastIdx = 0;
					for (T t : curr) {
						int idx = toIdx.applyAsInt(t);
						serUtils.writeVulong(out, idx - lastIdx);
						lastIdx = idx;
					}
				}
				return true;
			}

			final LCSResult<T> lcsResult = lcs.sequence(prev, curr);
			if (!lcsResult.actions.isEmpty()) {
				serUtils.writeVulong(out, curr.size());
				Iterator<T> diffItr = lcsResult.diffs.iterator();
				for (Action action : lcsResult.actions) {
					writeAction(out, action);
					if (LCS.actionWithDiff.contains(action.type)) {
						if (action.length > 4) {
							intWriter.write(out, writer -> {
								for (int i = 0; i < action.length; i++) {
									T t = diffItr.next();
									int idx = toIdx.applyAsInt(t);
									writer.write(idx);
								}
							});
						}else {
							int lastIdx = 0;
							for (int i = 0; i < action.length; i++) {
								T t = diffItr.next();
								int idx = toIdx.applyAsInt(t);
								serUtils.writeVulong(out, idx - lastIdx);
								lastIdx = idx;
							}
							
						}
					}
				}
				return true;
			}
		} else if (!prev.isEmpty()) {
			serUtils.writeVulong(out, 0);
			return true;
		}
		return false;
	}

	private void writeAction(OutputStream out, Action action) throws IOException {
		final int a = action.type.bits + Math.min(action.length, OSHDB.ACTION_LENGTH_BITMASK);
		out.write(a);
		if (action.length >= OSHDB.ACTION_LENGTH_BITMASK) {
			final int length = action.length - OSHDB.ACTION_LENGTH_BITMASK;
			serUtils.writeVulong(out, length);
		}
	}

	private void addAll(ObjectArrayList<OSHDBTag> dest, Iterable<OSHDBTag> src) {
		src.forEach(dest::add);
	}

	private void swapTagLists() {
		ObjectArrayList<OSHDBTag> swap = prevTags;
		prevTags = tags;
		tags = swap;
		tags.clear();
	}
}
