package org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

import com.google.common.collect.Lists;

public class OSHWayReader extends OSHReader<OSMWay> {

	private static final OSMMember[] EMPTY_MEMBER_ARRAY = new OSMMember[0];
	private List<OSMMember> members = Lists.newArrayList();
	private List<OSMMember> auxMembers = Lists.newArrayList();
	
	private LongFunction<OSMMember> idxToMember;

	public void reset(InputStream in, int header, long id, long baseTimestamp, List<OSHDBTag> tags, List<OSMMember> members, LongToIntFunction idxToUid,
			LongFunction<OSHDBTag> idxToTag, LongFunction<OSMMember> idxToMember) {
		reset(in, header, id, baseTimestamp, tags, idxToUid, idxToTag);
		
		this.members.clear();
		if(members != null){
			this.members.addAll(members);
		}
		this.auxMembers.clear();
		
		this.idxToMember = idxToMember;
	}

	@Override
	protected OSMWay readExt(InputStream in, boolean changed) throws IOException {
		if (changed) {
			final List<OSMMember> newMembers = readLCSValue(in, members, auxMembers, idxToMember);
			auxMembers = members;
			members = newMembers;
		}

		return new OSMWay(id, version*((isVisible)?1:-1), new OSHDBTimestamp(timestamp), changeset, uid, ((isVisible)?toIntArray(tags):EMPTY_KEY_VALUE_ARRAY), ((isVisible)?toMemberArray(members):EMPTY_MEMBER_ARRAY));
	}
	
	protected OSMMember[] toMemberArray(List<OSMMember> members){
		final OSMMember[] array = new OSMMember[members.size()];
		return members.toArray(array);
	}

}
