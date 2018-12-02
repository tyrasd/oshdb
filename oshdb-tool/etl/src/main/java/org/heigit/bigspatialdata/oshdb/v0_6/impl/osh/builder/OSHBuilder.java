package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS.Action;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LCS.LCSResult;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;

import com.google.common.collect.Lists;

public class OSHBuilder {
	
	private static final EnumSet<LCS.ActionType> actionWithDiff = EnumSet.of(LCS.ActionType.ADD, LCS.ActionType.UPDATE);

	protected final SerializationUtils serUtils = new SerializationUtils();
	protected final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("oshbuilder", false);
	protected final LCS lcs = new LCS();
	
	// entity
	private int prevVersion = 0;
	private long prevTimestamp = 0;
	private int prevUid = 0;
	private long prevChangeset = 0;
	private List<OSHDBTag> prevTags;
	
	protected void reset(){
		prevVersion = OSHDB.sortOrder.equals(OSHDB.SortOrder.ASC)?0:2;
		prevTimestamp = 0;
		prevUid = 0;
		prevChangeset = 0;
		prevTags = Lists.newArrayList();
	}

	public void buildSingleEntity(OutputStream out, int header, OSMEntity entity) throws IOException{
		reset();
		if((header & OSHDB.OSM_HEADER_COMPLETE) == 0){
			final int version = entity.getVersion();
			serUtils.writeVulong(out, version);
		}
		
		final int uid = entity.getUserId();
		serUtils.writeVulong(out, uid);
		
		final long changeset = entity.getChangeset();
		serUtils.writeVulong(out, changeset);
	}
	
	protected int buildEntity(OutputStream out, OSMEntity entity, long baseTimestamp, LongToIntFunction uidToIdx,
			ToIntFunction<OSHDBTag> tagToIdx) throws IOException {
		
		int header = 0;
		
		final int version = entity.getVersion();
		final int dVersion = version - prevVersion;
		if (dVersion == OSHDB.sortOrder.dir) {
			header |= OSHDB.OSM_HEADER_COMPLETE;
		} else {
			serUtils.writeVulong(out, version);
		}
		prevVersion = version;
	

		final long timestamp = entity.getTimestamp().getRawUnixTimestamp() - baseTimestamp;
		final long dTimestamp = timestamp - prevTimestamp;
		serUtils.writeVslong(out, dTimestamp);
		prevTimestamp = timestamp;

		final int uid = entity.getUserId();
		if (uid != prevUid) {
			header |= OSHDB.OSM_HEADER_CHG_UID;
			int idx = uidToIdx.applyAsInt(uid);
			serUtils.writeVulong(out, idx);
		}
		prevUid = uid;

		final long changeset = entity.getChangeset();
		final long dChangeset = changeset - prevChangeset;
		if (dChangeset != 0) {
			header |= OSHDB.OSM_HEADER_CHG_CS;
			serUtils.writeVslong(out, dChangeset);
		}
		prevChangeset = changeset;

		if (entity.isVisible()) {
			header |= OSHDB.OSM_HEADER_VISIBLE;

			final List<OSHDBTag> tags = Lists.newArrayList(entity.getTags());
			if (writeLCSValue(out,tags, prevTags, tagToIdx)) {
				header |= OSHDB.OSM_HEADER_CHG_TAGS;
			}
			prevTags = tags;
		} else {
			//TODO what should happen here
		}

		
		return header;
	}

	protected <T extends Comparable<T>> boolean writeLCSValue(OutputStream out, List<T> curr, List<T> prev, ToIntFunction<T> toIdx)
			throws IOException {
		if (!curr.isEmpty()) {
			if (prev.isEmpty()) {
				serUtils.writeVulong(out, curr.size());
				intWriter.write(out, writer -> {
					for (T t : curr) {
						int idx = toIdx.applyAsInt(t);
						writer.write(idx);
					}
				});
				return true;
			}
			
			final LCSResult<T> lcsResult = lcs.sequence(prev,curr);
			if (!lcsResult.actions.isEmpty()) {
				serUtils.writeVulong(out, curr.size());
				Iterator<T> diffItr = lcsResult.diffs.iterator();
				for (Action action : lcsResult.actions) {
					writeAction(out, action);
					if (actionWithDiff.contains(action.type)) {
						intWriter.write(out, writer -> {
							for(int i=0; i< action.length; i++){
								T t = diffItr.next();
								int idx = toIdx.applyAsInt(t);
								writer.write(idx);
							}
						});
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
		final int a = action.type.bits +Math.min(action.length, OSHDB.ACTION_LENGTH_BITMASK);
		out.write(a);
		if(action.length >= OSHDB.ACTION_LENGTH_BITMASK){
			final int length = action.length - OSHDB.ACTION_LENGTH_BITMASK;
			serUtils.writeVulong(out, length);
		}
	}

}
