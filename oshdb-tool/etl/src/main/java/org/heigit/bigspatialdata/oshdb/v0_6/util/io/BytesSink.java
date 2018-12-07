package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface BytesSink extends Closeable {
	
	long write(long id, ByteBuffer bytes) throws IOException;

}
