package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.orc.impl.SerializationUtils;

import com.google.common.io.CountingOutputStream;

public class FullIndexByteStore {

	private static final long GB = 1L * 1024L * 1024L * 1024L;
	private static final byte[] EMPTY_ARRAY = new byte[1024];

	private static final long pageSize = GB;
	private static final long pageMask = pageSize - 1;

	public static final BytesSource getSource(String path) throws FileNotFoundException, IOException {
		return new Source(path);
	}

	public static final BytesSink getSink(String path) throws FileNotFoundException {
		final OutputStream indexOut = new BufferedOutputStream(new FileOutputStream(path + ".index"));
		final OutputStream dataOut = new BufferedOutputStream(new FileOutputStream(path + ".data"));
		return new Sink(indexOut, dataOut);
	}

	private static class Source implements BytesSource {

		private final ByteBufferBackedInputStream input = new ByteBufferBackedInputStream();
		private final List<ByteBuffer> indexMappings;
		private final List<ByteBuffer> dataMappings;

		private Source(String path) throws FileNotFoundException, IOException {
			indexMappings = map(path + ".index");
			dataMappings = map(path + ".data");
		}

		@Override
		public ByteBuffer get(long id) throws IOException {
			final long idPos = id * 8;
			final int indexMapId = (int) (idPos / pageSize);
			final int indexOffset = (int) (idPos & pageMask);

			final long dataPos = indexMappings.get(indexMapId).getLong(indexOffset);
			final int dataMapId = (int) (dataPos / pageSize);
			final int dataOffset = (int) (dataPos & pageMask);

			ByteBuffer buffer = dataMappings.get(dataMapId);
			buffer.position(dataOffset);

			input.set(buffer);

			int size = (int) SerializationUtils.readVulong(input);
			buffer = buffer.slice();
			buffer.limit(size);
			return buffer;
		}

		private List<ByteBuffer> map(String path) throws FileNotFoundException, IOException {
			List<ByteBuffer> mappings;
			try (RandomAccessFile file = new RandomAccessFile(path, "r"); FileChannel channel = file.getChannel()) {
				long remaining = file.length();

				mappings = new ArrayList<>(Math.toIntExact((remaining / pageSize) + 1));

				long pos = 0;
				while (remaining > 0) {
					int size = (int) Math.min(remaining, pageSize);
					ByteBuffer buffer = channel.map(MapMode.READ_ONLY, pos, size);
					mappings.add(buffer);
					pos += size;
					remaining -= size;
				}
			}
			return mappings;
		}
	}

	private static class Sink implements BytesSink {

		private final SerializationUtils serUtil = new SerializationUtils();
		private final DataOutputStream outIndex;
		private final CountingOutputStream outData;

		private long pageLeft;
		private long nextId = 0;

		private Sink(OutputStream outIndex, OutputStream outData) {
			this.outIndex = new DataOutputStream(outIndex);
			this.outData = new CountingOutputStream(outData);
			this.pageLeft = pageSize;
		}

		@Override
		public long write(long id, ByteBuffer bytes) throws IOException {
			for (; nextId < id; nextId++) {
				outIndex.writeLong(-1);
			}

			if ((pageLeft - bytes.limit()) < 0) {
				// padding
				while (pageLeft > 0) {
					int len = (int) Math.min(EMPTY_ARRAY.length, pageLeft);
					outData.write(EMPTY_ARRAY, 0, len);
					pageLeft -= len;
				}
				pageLeft = pageSize;
			}

			long pos = outData.getCount();
			outIndex.writeLong(pos);
			serUtil.writeVulong(outData, bytes.limit());
			outData.write(bytes.array(), 0, bytes.limit());
			nextId++;
			return pos;
		}

		@Override
		public void close() throws IOException {
			try {
				outIndex.close();
			} catch (IOException e) {
				throw e;
			} finally {
				outData.close();
			}
		}

	}
}
