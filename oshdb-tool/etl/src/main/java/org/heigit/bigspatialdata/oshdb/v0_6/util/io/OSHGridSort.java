package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;

public class OSHGridSort implements Closeable {
	
	public interface SortKey<T> extends Comparable<T> {
		long sortKey();
		long oshId();
		long pos();
	}

	private final SerializationUtils serUtil = new SerializationUtils();
	
	private final int maxSize;
	private final ArrayList<SortKey> gridSort;
	private final String gridSortPath;
	private int sequence = 0;
	
	public OSHGridSort(int maxSize, String gridSortPath){
		this.maxSize = maxSize;
		this.gridSort = null; //new ArrayList<>(maxSize);
		this.gridSortPath = gridSortPath;
	}
	
	public void add(SortKey key) throws FileNotFoundException, IOException{
//		gridSort.add(key);
//		if(gridSort.size() >= maxSize){		
//			writeOut();
//			gridSort.clear();
//		}
	}
	
	@Override
	public void close() throws IOException {
	//	writeOut();
	}
	
	private void writeOut() throws FileNotFoundException, IOException{
		try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(String.format("%s_%04d", gridSortPath,sequence++))))){
			Iterator<SortKey> gridItr = gridSort.parallelStream().sorted().iterator();
			SortKey key = gridItr.next();
			long sortKey = key.sortKey();
			
			long oshId = key.oshId() + 1; // avaid 0 oshId!
			long pos = key.pos();
			
			serUtil.writeVulong(out,sortKey);
			serUtil.writeVulong(out,oshId);
			serUtil.writeVulong(out,pos);
			
			while(gridItr.hasNext()){
				key = gridItr.next();
				if(key.sortKey() != sortKey){
					serUtil.writeVulong(out,0);
					final long sortKeyDelta = key.sortKey() - sortKey;
					serUtil.writeVulong(out,sortKeyDelta);
					sortKey = key.sortKey();
				}
			
				oshId = key.oshId() + 1; // avid 0 oshId!
				pos = key.pos();
				serUtil.writeVulong(out,oshId);
				serUtil.writeVulong(out,pos);
			}					
		}
	}
}
