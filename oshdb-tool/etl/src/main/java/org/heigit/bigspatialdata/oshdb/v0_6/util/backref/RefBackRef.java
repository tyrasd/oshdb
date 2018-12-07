package org.heigit.bigspatialdata.oshdb.v0_6.util.backref;


public class RefBackRef implements Comparable<RefBackRef> {
	public final long ref;
	public final long backRef;

	public RefBackRef(long ref, long backRef) {
		this.ref = ref;
		this.backRef = backRef;
	}

	@Override
	public int compareTo(RefBackRef o) {
		int c = Long.compare(ref, o.ref);
		if (c == 0)
			c = Long.compare(backRef, o.backRef);
		return c;
	}
}