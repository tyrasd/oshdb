package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class OSHGridSort implements Closeable {
	
	public interface SortKey<T> extends Comparable<T> {
		long sortKey();
		long oshId();
		long pos();
	}

	private final SerializationUtils serUtil = new SerializationUtils();
	private final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("", false);
	
	private final int maxSize;
	private final ArrayList<SortKey> gridSort;
	private final String gridSortPath;
	private int sequence = 0;
	
	public OSHGridSort(int maxSize, String gridSortPath){
		this.maxSize = maxSize;
		this.gridSort = new ArrayList<>(maxSize);
		this.gridSortPath = gridSortPath;
	}
	
	public void add(SortKey key) throws FileNotFoundException, IOException{
		gridSort.add(key);
		if(gridSort.size() >= maxSize){		
			writeOut();
			gridSort.clear();
		}
	}
	
	@Override
	public void close() throws IOException {
		writeOut();
	}
	
	private void writeOut() throws FileNotFoundException, IOException{
		try(OutputStream out = new BufferedOutputStream(new FileOutputStream(String.format("%s_%04d", gridSortPath,sequence++)))){
			PeekingIterator<SortKey> gridItr = Iterators.peekingIterator(gridSort.parallelStream().sorted().iterator());
			final int BATCH = 255;	
			LongArrayList outId = new LongArrayList(BATCH);
			LongArrayList outPos = new LongArrayList(BATCH);
			long lastKey = 0;
			while(gridItr.hasNext()){
				long key = gridItr.peek().sortKey();
				
				while(gridItr.hasNext() && gridItr.peek().sortKey() == key){
					SortKey sk = gridItr.next();
					outId.add(sk.oshId());
					outPos.add(sk.pos());
					if(outId.size() == 255){
						break;
					}
				}
				writeBatch(out,key - lastKey,outId,outPos);	
				lastKey = key;
			}
			
		}
	}
	
	private void writeBatch(OutputStream out, long key, LongArrayList outId, LongArrayList outPos) throws IOException{
		serUtil.writeVulong(out, key);
		out.write(outId.size());
		if(outId.size() > 4){
			intWriter.write(out, writer -> {
				for(long l : outId){
					writer.write(l);
				}
			});
			intWriter.write(out, writer -> {
				for(long l : outPos){
					writer.write(l);
				}
			});
		}else{
			long ll = 0;
			for(long l : outId){
				serUtil.writeVulong(out, l - ll);
				ll = 0;
			}
			ll = 0;
			for(long l : outPos){
				serUtil.writeVulong(out, l - ll);
				ll = 0;
			}
		}
	}
	
}
