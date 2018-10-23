package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.plain;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;

import com.google.common.io.CountingOutputStream;

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

	private final ByteBuffer zoomCellId = ByteBuffer.allocate(5);

	private final DataOutputStream idCellOutIdx;
	private final CountingOutputStream idCellOut;

	public PlainIdToCellSink(OutputStream idCellOutIdx, OutputStream idCellOut) {
		this(DEFAULT_PAGE_SIZE, idCellOutIdx, idCellOut);
	}

	public PlainIdToCellSink(long pageSize, OutputStream idCellOutIdx, OutputStream idCellOut) {
		this.pageSize = Long.highestOneBit(pageSize - 1) << 1;
		this.pageMask = pageSize - 1;
		this.page = new long[Math.toIntExact(pageSize)];
		this.idCellOutIdx = new DataOutputStream(idCellOutIdx);
		this.idCellOut = new CountingOutputStream(idCellOut);
	}

	@Override
	public void close() throws IOException {
		flushPage();
		idCellOutIdx.close();
		idCellOut.close();
	}

	@Override
	public void put(long key, long cellId) throws IOException {
		if(key == 103561L){
			System.out.println("debug IdToCellSink put "+key);
		}
			
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
		long filePos = idCellOut.getCount();
		for (int off = 0; off < page.length; off++){
			long cellId = page[off];
			final int z = ZGrid.getZoom(cellId);
			final int id = (cellId == -1) ? -1 : Math.toIntExact(ZGrid.getIdWithoutZoom(cellId));
			zoomCellId.clear();
			zoomCellId.put((byte) z);
			zoomCellId.putInt(id);
			idCellOut.write(zoomCellId.array(), 0, zoomCellId.capacity());
		}
		idCellOutIdx.writeInt(pageIndex);
		idCellOutIdx.writeLong(filePos);

		Arrays.fill(page, 0);
		pageFillSize = 0;
	}

}
