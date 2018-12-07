package org.heigit.bigspatialdata.oshdb.v0_6.transform;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.RuntimeErrorException;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderCellData;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.OSHNodeReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.BackRefReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.RefToBackRefs;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.ByteBufferBackedInputStream;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.BytesSource;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.FullIndexByteStore;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Streams;
import com.google.common.io.MoreFiles;

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class Test {
	
	private static final long GB = 1L*1024L*1024L*1024L;
	private static final long pageSize = GB;
	private static final long pageMask = pageSize -1;

	
	private static <T extends Comparable<T>> PeekingIterator<T> merge(Path workDir, String glob, Function<Path,Iterator<T>> get) throws IOException {
		List<Iterator<T>> readers = Lists.newArrayList();
		for (Path path : Files.newDirectoryStream(workDir, glob)) {
			readers.add(get.apply(path));
		}
		return Iterators.peekingIterator(Iterators.mergeSorted(readers,(a,b) -> a.compareTo(b)));
	}
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		final Path dataDir = Paths.get("/home/rtroilo/data");
		final Path workDir = dataDir.resolve("work/planet");
		
		
		PeekingIterator<RefToBackRefs> itr = merge(workDir,"backRefs_node_relation",path -> {
			try {
				return BackRefReader.of(path);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		
		
		while(itr.hasNext() && itr.peek().ref < 2){
			itr.next();
		}
		if(itr.hasNext() && itr.peek().ref == 2){
			System.out.println(Arrays.toString(itr.next().backRefs));
		}
		
		
		if(true)
			return;
		
		
		try(OutputStream output = MoreFiles.asByteSink(workDir.resolve("backRefs_relation_relation")).openBufferedStream()){
			SerializationUtils serUtil = new SerializationUtils();
			DeltaIntegerWriter intWriter = new DeltaIntegerWriter("", false);
			List<long[]> backRefs = Lists.newArrayList();
			long lastRef = 0;
			System.out.println("start ...");
			Stopwatch stopwatch = Stopwatch.createStarted();
			int maxSize = 0;
			while(itr.hasNext()){
				long ref = itr.peek().ref;
				int size = 0;
				while(itr.hasNext() && itr.peek().ref == ref){
					long [] br = itr.next().backRefs;
					size += br.length;
					backRefs.add(br);
				}
				
				if(maxSize < size){
					String s = backRefs.stream().map(Arrays::toString).collect(Collectors.joining(",", "(", ")"));
					System.out.println("ref "+ref+" with "+size+" backRefs: "+s);
					maxSize = size;
				}
				
				serUtil.writeVulong(output, ref - lastRef);
				serUtil.writeVulong(output, size);
				intWriter.write(output, writer -> {
					for(long[] br : backRefs){
						for(long l : br){
							writer.write(l);
						}
					}
				});
								
				lastRef = ref;
				backRefs.clear();
			}
			System.out.println(stopwatch);
		}
			
		if(true)
			return;

		BytesSource bytesSource = FullIndexByteStore.getSource(workDir.resolve("x_node").toString());
		OSHNodeReader osh = new OSHNodeReader();
		
		long id = 2017050407L;
		
		ByteBuffer dataMapping = bytesSource.get(id);
				
		int size = dataMapping.limit();
		System.out.println("size:"+size);
		
		osh.read(id, dataMapping);
		
		Streams.stream(osh.iterator()).forEach(System.out::println);
			
	}
}
