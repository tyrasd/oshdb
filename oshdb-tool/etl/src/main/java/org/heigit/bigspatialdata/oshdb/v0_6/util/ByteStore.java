package org.heigit.bigspatialdata.oshdb.v0_6.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ByteStore {
	
	long write(long id, ByteBuffer bytes) throws IOException;

}
