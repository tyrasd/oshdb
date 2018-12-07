package org.heigit.bigspatialdata.oshdb.v0_6;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSHDBTimeSpan {
	
	private OSHDBTimestamp begin = new OSHDBTimestamp(Long.MAX_VALUE);
	private OSHDBTimestamp end = new OSHDBTimestamp(Long.MIN_VALUE);
	
	public OSHDBTimeSpan(long begin, long end){
		this.begin.setTimestamp(begin);
		this.end.setTimestamp(end);
	}

}
