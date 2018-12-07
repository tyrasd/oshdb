package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

public class OSHStoreSource {

	private final long pageSize;
	private final long pageMask;

	private final RandomAccessFile index = null;
	private final RandomAccessFile data = null;

	private final FileChannel indexChannel = null;
	private final FileChannel dataChannel = null;

	private final InputStream dataInput = null;

	private LongArrayList positions = new LongArrayList();

	private final LoadingCache<Long, LongBuffer> indexCache;
	private final LoadingCache<Long, ByteBuffer> dataCache;
	
	private final ByteBufferBackedInputStream input = new ByteBufferBackedInputStream();

	public OSHStoreSource(long pageSize, long maxCacheSize) {
		this.pageSize = Long.highestOneBit(pageSize - 1) << 1;
		this.pageMask = this.pageSize - 1;

		indexCache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build(new CacheLoader<Long, LongBuffer>() {
			@Override
			public LongBuffer load(Long pos) throws Exception {
				ByteBuffer buffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, pos.longValue(), pageSize);
				return buffer.asLongBuffer();
			}
		});

		dataCache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build(new CacheLoader<Long, ByteBuffer>() {
			@Override
			public ByteBuffer load(Long pos) throws Exception {
				ByteBuffer buffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, pos.longValue(), pageSize);
				return buffer;
			}
		});
	}

	public static class OSHIdBytes {
		public final OSHDBBoundingBox bbox = null;
	}

	public void read(LongList ids) throws IOException, ExecutionException {
		positions.ensureCapacity(ids.size());
		long lastIndexPageId = -1;
		long lastDataPageId = -1;
		LongBuffer indexPage = null;
		ByteBuffer dataPage = null;
		for (long id : ids) {
			long indexPageId = (id / pageSize);
			int indexOffset = Math.toIntExact(id & pageMask);

			if (indexPageId != lastIndexPageId) {
				indexPage = indexCache.get(indexPageId);
				lastIndexPageId = indexPageId;
			}

			long pos = indexPage.get(indexOffset);

			long dataPageId = (pos / pageSize);
			int dataOffset = Math.toIntExact(pos & pageMask);
			if(dataPageId != lastDataPageId){
				dataPage = dataCache.get(dataPageId);
				input.set(dataPage);
			}
			
			dataPage.position(dataOffset);		
			
			int size = (int) SerializationUtils.readVulong(input);
					
		}
	}

	class ByteBufferBackedInputStream extends InputStream {

		ByteBuffer buf;

		public void set(ByteBuffer buf) {
			this.buf = buf;
		}

		public int read() throws IOException {
			if (!buf.hasRemaining()) {
				return -1;
			}
			return buf.get();
		}

		public int read(byte[] bytes, int off, int len) throws IOException {
			len = Math.min(len, buf.remaining());
			buf.get(bytes, off, len);
			return len;
		}
	}

}
