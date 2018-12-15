package org.heigit.bigspatialdata.oshdb.tool.importer;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.LongFunction;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.Memory;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping.CellDataSink;

import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class CellDataMap implements CellDataSink {
	private static final int MAX_CONTAINER_SIZE = 1024 * 1024 * 1024; // GB

	private static class DataContainer {
		private long sizeInBytes;
		private LongArrayList offsets = new LongArrayList();

	}

	private final Path workDirectory;
	private final String spillFileName;
	private final Memory memory;
	private final Long2ObjectSortedMap<DataContainer> cellContainerMap;
	private long maxInternalMemory;
	private long memoryUsage;
	private int spillNumber = 0;

	public CellDataMap(Path workDirectory, String fileName, long maxMemory) {
		this.workDirectory = workDirectory;
		this.spillFileName = fileName;
		this.maxInternalMemory = (long) (maxMemory * 0.10); // 10% of maxMemory
															// for internal use
		memory = new Memory(maxMemory - maxInternalMemory);
		cellContainerMap = new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_BOTTOM_UP);
	}

	public void add(long cellId, ByteBuffer data) throws IOException {
		DataContainer c = cellContainerMap.get(cellId);

		if (c == null) {
			memoryUsage += 100;
		}

		if (memory.remaining() < data.limit() || ((memoryUsage + 8) >= maxInternalMemory)
				|| (c != null && c.sizeInBytes + data.limit() >= MAX_CONTAINER_SIZE)) {
			spillToDisk();
			c = new DataContainer();
			cellContainerMap.put(cellId, c);
			memoryUsage += 100;
		}

		if (c == null) {
			c = new DataContainer();
			cellContainerMap.put(cellId, c);
		}

		long pos = memory.pos();

		c.offsets.add(pos);
		c.sizeInBytes += data.limit();
		memory.putInt(data.limit());
		memory.put(data.array(), data.arrayOffset(), data.limit());
		memoryUsage += 8; // offset entry
	}

//	public void add(long cellId, long id, LongFunction<ByteBuffer> data) throws IOException {
//		DataContainer c = cellContainerMap.get(cellId);
//        long lastId = 0;
//		if (c == null) {
//
//			memoryUsage += 100; // we roughly estimate the container size +
//								// mapentry;
//		}else{
//			lastId = c.lastId;
//		}
//
//		ByteBuffer bb = data.apply(lastId);
//
//		if (memory.remaining() < bb.limit() || 
//			((memoryUsage + 8) >= maxInternalMemory) || 
//			(c != null && c.sizeInBytes + bb.limit() >= MAX_CONTAINER_SIZE)) {
//			spillToDisk();
//			c = new DataContainer();
//			cellContainerMap.put(cellId, c);
//			memoryUsage += 100;
//			bb = data.apply(c.lastId);
//
//		}
//
//		if (c == null) {
//			c = new DataContainer();
//			cellContainerMap.put(cellId, c);
//		}
//
//		long pos = memory.pos();
//		c.offsets.add(pos);
//		c.sizeInBytes += bb.limit();
//		c.lastId = id;
//
//		memory.putInt(bb.limit());
//		memory.put(bb.array(), bb.arrayOffset(), bb.limit());
//
//		memoryUsage += 8; // offset entry
//	}

	private void spillToDisk() throws IOException {
		final String fileName = String.format("%s_%03d", spillFileName, spillNumber++);
		final Path filePath = workDirectory.resolve(fileName);

		System.out.print("write to disk " + filePath + "  ");
		Stopwatch stopwatch = Stopwatch.createStarted();
		try (CountingOutputStream cout = new CountingOutputStream(
				Files.asByteSink(filePath.toFile()).openBufferedStream());
				DataOutputStream out = new DataOutputStream(cout)) {
			ObjectIterator<Entry<DataContainer>> iter = cellContainerMap.long2ObjectEntrySet().iterator();
			while (iter.hasNext()) {
				Entry<DataContainer> entry = iter.next();
				final long cellId = entry.getLongKey();
				final DataContainer container = entry.getValue();
				final long rawSize = container.sizeInBytes;
				if (container.offsets.size() == 0) {
					continue;
				}
				out.writeLong(cellId);
				out.writeInt(container.offsets.size());
				out.writeInt((int) rawSize);
				for (long offset : container.offsets) {
					int length = memory.getInt(offset);
					out.writeInt(length);
					memory.write(offset + 4, length, out);
				}
			}
			System.out.println(" done. Bytes " + cout.getCount() + " in " + stopwatch);
		}

		cellContainerMap.clear();
		memory.clear();
		memoryUsage = 0;
	}

	@Override
	public void close() throws IOException {
		spillToDisk();
	}

	@Override
	public String toString() {
		return "CellDataMap [memory=" + memory + ", maxInternalMemory=" + maxInternalMemory + ", memoryUsage="
				+ memoryUsage + ", cellContainerMap=" + cellContainerMap.size() + ", workDirectory=" + workDirectory
				+ ", spillFileName=" + spillFileName + ", spillNumber=" + spillNumber + "]";
	}

}
