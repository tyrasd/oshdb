package org.heigit.bigspatialdata.oshdb.v0_6.util.backref;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.MoreFiles;

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class BackRefSink implements Closeable {

	private final SerializationUtils serUtil = new SerializationUtils();
	private final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("", false);
	private final ArrayList<RefBackRef> refBackRefs;
	private final LongArrayList backRefs = new LongArrayList();
	private final int maxSize;
	private final String path;
	private int sequence;
		
	public BackRefSink(String path, int maxSize){
		this.path = path;
		this.maxSize = maxSize;
		refBackRefs = new ArrayList<>(maxSize);
	}
	
	public void add(long ref, long backRef) throws IOException{
		refBackRefs.add(new RefBackRef(ref,backRef));
		
		if(refBackRefs.size() >= maxSize){
			spill();
		}
	}
	
	@Override
	public void close() throws IOException {
		spill();
	}
	
	private void spill() throws IOException{
		final String filename = String.format("%s_%04d", path, sequence++);
		System.out.print("spill backref to "+filename+" ... ");
		Stopwatch stopwatch = Stopwatch.createStarted();
		try(OutputStream output = MoreFiles.asByteSink(Paths.get(filename)).openBufferedStream()){
			PeekingIterator<RefBackRef> itr = Iterators.peekingIterator(sort(refBackRefs));
			long lastRef = 0;
			while(itr.hasNext()){
				long ref = itr.peek().ref;
				while(itr.hasNext() && itr.peek().ref == ref){
					backRefs.add(itr.next().backRef);
				}
				
				serUtil.writeVulong(output, ref - lastRef);
				serUtil.writeVulong(output, backRefs.size());
				intWriter.write(output, writer -> {
					for(long backRef : backRefs){
						writer.write(backRef);
					}
				});
				
				lastRef = ref;
				backRefs.clear();
			}
		}
		refBackRefs.clear();
		System.out.println(stopwatch);
	}
	
	private Iterator<RefBackRef> sort(List<RefBackRef> list){
		return list.parallelStream().sorted().iterator();
	}
}
