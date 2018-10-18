package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.plain;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;

public class PlainIdToCellSink implements IdToCellSink {

	@FunctionalInterface
	public interface PageWriter {
		public void writeOut(int index, long[] page, long fillSize) throws IOException;
	}
	
	private static final long DEFAULT_PAGE_SIZE = 1024 * 1024;

	private final long pageSize;
	private final long pageMask;

	private final long[] page;
	private int pageIndex = 0;
	private int pageFillSize = 0;
	
	private final PageWriter pageWriter;
	

	public PlainIdToCellSink(PageWriter pageWriter){
		this(DEFAULT_PAGE_SIZE, pageWriter);
	}
	
	public PlainIdToCellSink(long pageSize, PageWriter pageWriter) {
		this.pageSize = Long.highestOneBit(pageSize - 1) << 1;
		this.pageMask = pageSize - 1;
		this.page = new long[Math.toIntExact(pageSize)];
		this.pageWriter = pageWriter;
	}

	@Override
	public void close() throws IOException {
		flushPage();
	}

	@Override
	public void put(long key, long cellId) throws IOException {
		int index = (int) (key / pageSize);
		int offset = (int) (key & pageMask);

		if (index < pageIndex) {
			throw new InvalidParameterException("keys must be in increasing order");
		}
		if (index > pageIndex) {
			flushPage();
			pageIndex = index;
		}
		page[offset] = cellId;
		pageFillSize++;
	}

	private void flushPage() throws IOException {
		pageWriter.writeOut(pageIndex,page, pageFillSize);
		Arrays.fill(page, 0);
		pageFillSize = 0;
	}

}
