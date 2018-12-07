package org.heigit.bigspatialdata.oshdb.v0_6.util;

import java.io.IOException;

public interface LongKeyValueSink {
	
	public void add(long key, long value) throws IOException;
}
