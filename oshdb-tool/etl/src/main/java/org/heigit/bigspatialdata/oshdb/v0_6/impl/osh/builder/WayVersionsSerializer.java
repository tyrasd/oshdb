package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class WayVersionsSerializer extends EntityVersionsSerializer {

	private final FastByteArrayOutputStream aux = new FastByteArrayOutputStream(4096);

	private ObjectArrayList<OSMMember> prevMembers = new ObjectArrayList<>();
	private ObjectArrayList<OSMMember> members = new ObjectArrayList<>();

	public void serialize(OutputStream output, List<OSMWay> versions, boolean visible, boolean single,
			boolean hasTags, LongToIntFunction uidToIdx, ToIntFunction<OSHDBTag> tagToIdx,
			ToIntFunction<OSMMember> memberToIdx) throws IOException {
		Iterator<OSMWay> itr = versions.iterator();
		OSMWay version = itr.next();

		first(output, version, single, hasTags, uidToIdx, tagToIdx);

		members.clear();
		if (version.isVisible()) {
			addAll(members, version.getMembers());
			serUtils.writeVulong(output, members.size());
			if (members.size() > 4) {
				intWriter.write(output, writer -> {
					for (OSMMember m : members) {
						int idx = memberToIdx.applyAsInt(m);
						writer.write(idx);
					}
				});
			}else {
				int lastIdx = 0;
				for (OSMMember m : members) {
					int idx = memberToIdx.applyAsInt(m);
					serUtils.writeVulong(output, idx - lastIdx);
					lastIdx = idx;
				}
			}
		}
		swapMemberLists();

		while (itr.hasNext()) {
			aux.reset();
			version = itr.next();

			int header = 0;
			while (((header = serializeEntity(aux, version, uidToIdx, tagToIdx)) & OSHDB.OSM_HEADER_MISSING) != 0) {
				output.write(header);
			}

			if (version.isVisible()) {
				addAll(members, version.getMembers());
				if (writeLCSValue(aux, members, prevMembers, memberToIdx)) {
					header |= OSHDB.OSM_HEADER_CHG_EXT;
				}
				swapMemberLists();
			}

			if (!itr.hasNext()) {
				header |= OSHDB.OSM_HEADER_END;
			}

			output.write(header);
			output.write(aux.array, 0, aux.length);
		}
	}

	private void addAll(ObjectArrayList<OSMMember> dest, Iterable<OSMMember> src) {
		for (OSMMember m : src) {
			dest.add(m);
		}
	}

	private void swapMemberLists() {
		ObjectArrayList<OSMMember> swap = prevMembers;
		prevMembers = members;
		members = swap;
		members.clear();
	}
}
