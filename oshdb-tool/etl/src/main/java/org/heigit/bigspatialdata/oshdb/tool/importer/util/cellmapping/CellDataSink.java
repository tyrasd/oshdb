package org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface CellDataSink extends Closeable {

	public void add(long cellId, ByteBuffer data) throws IOException;

}
