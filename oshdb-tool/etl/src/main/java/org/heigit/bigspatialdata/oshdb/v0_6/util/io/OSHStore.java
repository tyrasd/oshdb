package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.v0_6.util.ByteStore;

import com.google.common.io.CountingOutputStream;

public class OSHStore implements ByteStore {
	private static final long GB = 1L * 1024L * 1024L * 1024L;
	private static final byte[] EMPTY_ARRAY = new byte[1024];
	
	private final SerializationUtils serUtil = new SerializationUtils();
	private final DataOutputStream outIndex;
	private final CountingOutputStream outData;
	private final long pageSize;
	
	private long pageLeft;
	private long nextId = 0;
	
	public OSHStore(OutputStream outIndex, OutputStream outData) {
		this(outIndex,outData,1L*GB);
	}
	
	private OSHStore(OutputStream outIndex, OutputStream outData, long pageSize) {
		this.outIndex = new DataOutputStream(outIndex);
		this.outData = new CountingOutputStream(outData);
		this.pageSize = pageSize;
		this.pageLeft = pageSize;
	}


	public long write(long id, ByteBuffer bytes) throws IOException{
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
}
