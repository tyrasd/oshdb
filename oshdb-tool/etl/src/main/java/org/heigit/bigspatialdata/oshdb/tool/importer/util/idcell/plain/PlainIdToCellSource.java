package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.plain;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import com.google.common.io.MoreFiles;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class PlainIdToCellSource implements IdToCellSource {

	private static final long DEFAULT_PAGE_SIZE = 1024 * 1024;

	private final long pageSize;
	private final long pageMask;

	private final Loader indexBufferMap;

	private static interface Loader extends Closeable {
		public ByteBuffer[] get(Integer index);
	}

	public static PlainIdToCellSource get(Path workDir, String idCellIdxGlob, boolean mapped) throws IOException {
		return get(DEFAULT_PAGE_SIZE, workDir, idCellIdxGlob, mapped);
	}

	public static PlainIdToCellSource get(long pageSize, Path workDir, String idCellIdxGlob, boolean mapped)
			throws IOException {
		long bufferSize = pageSize * 5;
		Map<Integer, ByteBuffer[]> indexBufferMap = Maps.newHashMap();
		ByteBuffer buffer = null;
		int index = -1;

		List<Path> indexPaths = Lists.newArrayList(Files.newDirectoryStream(workDir, idCellIdxGlob));
		indexPaths.sort(Comparator.naturalOrder());
		System.out.println("loading index buffers");
		for (Path pIndex : indexPaths) {
			System.out.println(pIndex);
			String pMap = pIndex.toString();
			try (DataInputStream indexIn = new DataInputStream(MoreFiles.asByteSource(pIndex).openBufferedStream());
					RandomAccessFile raf = new RandomAccessFile(pMap.substring(0, pMap.length() - 4), "r");
					FileChannel channel = raf.getChannel()) {
				while (indexIn.available() > 0) {
					int idx = indexIn.readInt();
					long pos = indexIn.readLong();
					ByteBuffer buf;
					
				
					
					if (mapped) {
						buf = channel.map(FileChannel.MapMode.READ_ONLY, pos, bufferSize);
					} else {
						buf = ByteBuffer.allocateDirect((int) bufferSize);
						channel.read(buf);
					}
					if (idx == index) {
						indexBufferMap.put(index, new ByteBuffer[] { buffer, buf });
					} else {
						if (buffer != null) {
							if(!indexBufferMap.containsKey(index))
								indexBufferMap.put(index, new ByteBuffer[] { buffer });
						}
						buffer = buf;
					}
					index = idx;
				}
			}
		}
		indexBufferMap.put(index, new ByteBuffer[] { buffer });
		System.out.println("index loaded " + indexBufferMap.size());
		Loader loader = new Loader() {
			@Override
			public void close() throws IOException {
				indexBufferMap.clear();
			}

			@Override
			public ByteBuffer[] get(Integer index) {
				return indexBufferMap.get(index);
			}
		};

		return new PlainIdToCellSource(pageSize, loader);
	}

	public PlainIdToCellSource(Loader indexBufferMap) {
		this(DEFAULT_PAGE_SIZE, indexBufferMap);
	}

	public PlainIdToCellSource(long pageSize, Loader indexBufferMap) {
		this.pageSize = Long.highestOneBit(pageSize - 1) << 1;
		this.pageMask = pageSize - 1;
		this.indexBufferMap = indexBufferMap;
	}

	@Override
	public void close() throws IOException {
		indexBufferMap.close();
	}

	@Override
	public long get(long id) throws IOException {
		int idx = (int) (id / pageSize);
		int off = ((int) (id & pageMask)) * 5;

		ByteBuffer[] pages = indexBufferMap.get(idx);
		if(pages == null){
			return -1;
		}

		ByteBuffer page = pages[0].duplicate();
		if (off > page.limit()) {
			System.out.printf("id:%10d idx:%4d off:%6d page:%s pages:%s%n", id, idx, off, page, Arrays.toString(pages));
		}
		page.position(off);
		int zoom = page.get();
		long cellId = page.getInt();

		if (pages.length > 1) {
			page = pages[1].duplicate();
			int z = page.get();
			long i = page.getInt();
			if (z > zoom || i > cellId) {
				zoom = z;
				cellId = i;
			}
		}

		return ZGrid.addZoomToId(cellId, zoom);
	}

	@Override
	public LongSet get(LongSortedSet ids) throws IOException {
		LongSet ret = new LongArraySet(ids.size());

		ByteBuffer[] pages = new ByteBuffer[0];
		int lastIdx = -1;
		long lastCellId = Long.MIN_VALUE;

		for (long id : ids) {
			int idx = (int) (id / pageSize);
			int off = ((int) (id & pageMask)) * 5;

			if (lastIdx != idx) {
				pages = indexBufferMap.get(idx);
				lastIdx = idx;
			}
			
			if(pages == null){
				continue;
			}

			ByteBuffer page = pages[0].duplicate();
			if (off > page.limit()) {
				System.out.printf("id:%10d idx:%4d off:%6d page:%s pages:%s%n", id, idx, off, page,
						Arrays.toString(pages));
			}
			page.position(off);
			int zoom = page.get();
			long cellId = page.getInt();

			if (pages.length > 1) {
				page = pages[1].duplicate();
				page.position(off);
				int z = page.get();
				long i = page.getInt();
				if (z > zoom || i > cellId) {
					zoom = z;
					cellId = i;
				}
			}
			cellId = ZGrid.addZoomToId(cellId, zoom);
			if (cellId != lastCellId) {
				ret.add(cellId);
			}
		}

		return ret;
	}

}
