package org.heigit.bigspatialdata.oshdb.tool.importer;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.google.common.base.Stopwatch;
import com.google.common.io.Files;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class CellRefMap implements Closeable {
	
	private static class BitmapContainer {
		Roaring64NavigableMap nodes;
		boolean dirtyNodes = false;
		long sizeNodes = 0;
		Roaring64NavigableMap ways;
		boolean dirtyWays = false;
		long sizeWays = 0;
	}

	private final Path workDirectory;
	private final String spillFileName;
	
	private final Long2ObjectSortedMap<BitmapContainer> cellContainerMap;
	private final long maxMemory;
	private long memoryUsage;
	private int spillNumber = 0;
	
	private int counter = 0;
	private int checkInterval = 10_000;
	
	public CellRefMap(Path workDirectory, String fileName, long maxMemory) {
		this.workDirectory = workDirectory;
		this.spillFileName = fileName;
		this.maxMemory = maxMemory;
		cellContainerMap = new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN);
	}
	
	public void add(long cellId, LongSet nodes, LongSet ways) throws IOException{
		BitmapContainer c = cellContainerMap.get(cellId);
		if(c == null){
			c = new BitmapContainer();
			cellContainerMap.put(cellId, c);
			memoryUsage += 100; // we roughly estimate the container size + mapentry;
		}
		
		if(nodes != null && !nodes.isEmpty()){
			if(c.nodes == null){
				c.nodes = new Roaring64NavigableMap();
			}
			Roaring64NavigableMap bitmap = c.nodes;
			nodes.forEach((long it) -> bitmap.add(it));
			c.dirtyNodes = true;
		}
		if(ways != null && !ways.isEmpty()){
			if(c.ways == null){
				c.ways = new Roaring64NavigableMap();
			}
			Roaring64NavigableMap bitmap = c.ways;
			ways.forEach((long it) -> bitmap.add(it));
			c.dirtyWays = true;
		}
		
		counter++;
		if((counter % checkInterval) == 0){
			for(BitmapContainer test : cellContainerMap.values()){
				if(test.dirtyNodes){
					memoryUsage -= test.sizeNodes;
					test.nodes.runOptimize();
					test.sizeNodes = test.nodes.getLongSizeInBytes();
					memoryUsage += test.sizeNodes;
					test.dirtyNodes = false;
				}
				if(test.dirtyWays){
					memoryUsage -= test.sizeWays;
					test.ways.runOptimize();
					test.sizeWays = test.ways.getLongSizeInBytes();
					memoryUsage += test.sizeWays;
					test.dirtyWays = false;
				}
			}
			
			if(memoryUsage > maxMemory){
				spillToDisk();
			}
		}
	}
	
	private void spillToDisk() throws IOException{
		final String fileName = String.format("%s_%03d", spillFileName, spillNumber++);
		final Path filePath = workDirectory.resolve(fileName);
		
		System.out.print("write to disk "+filePath+"  ");
		Stopwatch stopwatch = Stopwatch.createStarted();
		long written = 0;
		try(DataOutputStream out = new DataOutputStream(Files.asByteSink(filePath.toFile()).openBufferedStream())){
			ObjectIterator<Entry<BitmapContainer>> iter = cellContainerMap.long2ObjectEntrySet().iterator();
			while (iter.hasNext()) {
				Entry<BitmapContainer> entry = iter.next();
				final long cellId = entry.getLongKey();
				final BitmapContainer container = entry.getValue();
				
				long sizeNodes = 0;
				long sizeWays = 0;
				int flag = 0;
				if(container.nodes != null){
					if(container.dirtyNodes)
						container.nodes.runOptimize();
					sizeNodes = container.nodes.serializedSizeInBytes();
					if(sizeNodes > 0)
						flag += 1;	
				}
				if(container.ways != null){
					if(container.dirtyWays)
						container.ways.runOptimize();
					sizeWays = container.ways.serializedSizeInBytes();
					if(sizeWays > 0)
						flag += 2;
				}
				
				out.writeLong(cellId);
				out.writeLong((sizeNodes+sizeWays)+1);
				out.write(flag);;
				if(sizeNodes > 0){
					container.nodes.serialize(out);
				}
				out.writeLong(sizeWays);
				if(sizeWays > 0){
					container.ways.serialize(out);
				}
			}
		}
		System.out.println(" done. Bytes "+written+" in "+stopwatch);
		cellContainerMap.clear();
		memoryUsage = 0;
	}
	
	@Override
	public void close() throws IOException {
		spillToDisk();
		
	}

}
