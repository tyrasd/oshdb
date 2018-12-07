package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.heigit.bigspatialdata.oshdb.v0_6.util.LongKeyValueSink;

public class FullIndexStore {
	
	public static LongKeyValueSink sink(){
		return null;
	}
	
	
	private static class Sink implements LongKeyValueSink, Closeable {
		
		private final DataOutputStream outIndex;
		
		private long nextId = 0;
		
		private Sink(OutputStream out){
			this.outIndex = new DataOutputStream(out);
		}
		
		@Override
		public void add(long key, long value) throws IOException {
			for (; nextId < key; nextId++) {
				outIndex.writeLong(-1);
			}
			outIndex.writeLong(value);
		}
		
		@Override
		public void close() throws IOException {
			outIndex.close();
		}
		
	}

}
