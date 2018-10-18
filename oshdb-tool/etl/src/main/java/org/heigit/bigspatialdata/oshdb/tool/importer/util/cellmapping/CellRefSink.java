package org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping;

import java.io.Closeable;
import java.io.IOException;

import it.unimi.dsi.fastutil.longs.LongSet;

public interface CellRefSink extends Closeable {
	
	public void add(long cellId, LongSet nodes, LongSet ways) throws IOException;

}
