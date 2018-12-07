package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BytesSource {

	ByteBuffer get(long id) throws IOException;
	
}
