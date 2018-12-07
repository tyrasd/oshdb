package org.heigit.bigspatialdata.oshdb.v0_6;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellBitmaps;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderCellData;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.MoreFiles;

public class LoadIgnite {
	
	public static class GridIdPos {
		public final long key;
		public final long id;
		public final long pos;
		
		public GridIdPos(long key, long id, long pos){
			this.key = key;
			this.id = id;
			this.pos = pos;
		}

		@Override
		public String toString() {
			return "GridIdPos [key=" + key + ", id=" + id + ", pos=" + pos + "]";
		}
		
		
	}
	
	public static class GridSortReader implements Iterator<GridIdPos> {

		private final InputStream in;
		private long key = -1;
		
		private GridIdPos next = null;
		
		public GridSortReader(Path path) throws IOException{
			in = MoreFiles.asByteSource(path).openBufferedStream();
			key = SerializationUtils.readVulong(in);
		}
		
		@Override
		public boolean hasNext() {
			try {
				return next != null || (next = getNext()) != null;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}

		@Override
		public GridIdPos next() {
			if(!hasNext()){
				throw new NoSuchElementException();
			}
			GridIdPos ret = next;
			next = null;
			return ret;
		}
		
		private GridIdPos getNext() throws IOException{
			if(in.available() > 0){
				long id = SerializationUtils.readVulong(in);
				if(id == 0){
					key += SerializationUtils.readVulong(in);
					id = SerializationUtils.readVulong(in);
				}
				long pos = SerializationUtils.readVulong(in);
				
				return new GridIdPos(key, id -1, pos);
			}
			return null;
		}
	}
	
	
	private static PeekingIterator<GridIdPos> merge(Path workDir, String glob) throws IOException {
		List<GridSortReader> readers = Lists.newArrayList();
		for (Path path : Files.newDirectoryStream(workDir, glob)) {
			readers.add(new GridSortReader(path));
		}
		return Iterators.peekingIterator(Iterators.mergeSorted(readers, (a, b) -> {
			int c = Long.compare(a.key, b.key);
			return c;
		}));
	}
	

	public static void main(String[] args) throws IOException {
		
		final Path dataDir = Paths.get("/home/rtroilo/data");
		final Path workDir = dataDir.resolve("work/planet");
		final String prefix = "x_node";
		
		PeekingIterator<GridIdPos> gridSortReaders = merge(workDir, "x_node.grid_*");
		long count = 0;
		long key = gridSortReaders.peek().key;
		while(gridSortReaders.hasNext()){
			count = 0;
			while(gridSortReaders.peek().key == key){
				gridSortReaders.next();
				count++;
			}
			key = gridSortReaders.peek().key;
			System.out.printf("%12d - %10d%n",key,count);
		}
		
		System.out.printf("%12d - %10d%n",key,count);
	}
}
