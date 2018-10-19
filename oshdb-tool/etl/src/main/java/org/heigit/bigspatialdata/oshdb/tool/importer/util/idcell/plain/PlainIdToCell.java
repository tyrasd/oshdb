package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.plain;

import java.io.IOException;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class PlainIdToCell implements IdToCellSink,IdToCellSource {
	
	private static final long DEFAULT_PAGE_SIZE = 1024*1024;
	
	private final long pageSize;
	private final long pageMask;
	
	
	public PlainIdToCell(long pageSize){
		this.pageSize = Long.highestOneBit(pageSize -1 ) << 1;
		this.pageMask = pageSize -1;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long get(long id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public LongSet get(LongSortedSet idss) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(long id, long cellId) throws IOException {
		// TODO Auto-generated method stub
		
	}

	
	
}
