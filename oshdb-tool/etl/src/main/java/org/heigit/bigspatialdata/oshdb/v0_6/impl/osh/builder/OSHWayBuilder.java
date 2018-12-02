package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;


import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class OSHWayBuilder extends OSHBuilder {
	private final FastByteArrayOutputStream aux = new FastByteArrayOutputStream(4096);

	private List<OSMMember> prevMembers = Collections.emptyList();

	public void build(OutputStream out, List<OSMWay> versions, long baseTimestamp, LongToIntFunction uidToIdx,
			ToIntFunction<OSHDBTag> tagToIdx, ToIntFunction<OSMMember> memberToIdx) throws IOException {

		super.reset();
		aux.reset();
		prevMembers.clear();

		final Iterator<OSMWay> itr = versions.iterator();
		while (itr.hasNext()) {
			final OSMWay version = itr.next();

			int header = buildEntity(aux, version, baseTimestamp, uidToIdx, tagToIdx);

			if (version.isVisible()) {
				final List<OSMMember> members = Lists.newArrayList(version.getRefs());
				if (writeLCSValue(aux,members, prevMembers, memberToIdx)) {
					header |= OSHDB.OSM_HEADER_CHG_EXT;
				}
				prevMembers = members;
			}
			
			if(!itr.hasNext()) {
				header |= OSHDB.OSM_HEADER_END;
			}
			
			out.write(header);
			out.write(aux.array, 0, aux.length);
		}
	}
}
