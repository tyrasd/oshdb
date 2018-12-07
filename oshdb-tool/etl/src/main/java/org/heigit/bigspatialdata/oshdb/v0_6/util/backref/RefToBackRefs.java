package org.heigit.bigspatialdata.oshdb.v0_6.util.backref;

public class RefToBackRefs implements Comparable<RefToBackRefs> {
	public final long ref;
	public final long[] backRefs;
	
	public RefToBackRefs(long ref, long[] backRefs) {
		this.ref = ref;
		this.backRefs = backRefs;
	}
	
	@Override
	public int compareTo(RefToBackRefs o) {		
		int c = Long.compare(ref, o.ref);
		if( c ==  0)
			c = Long.compare(backRefs[0], o.backRefs[0]);
		return c;
	}
}
