package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

import com.google.common.io.CountingOutputStream;
import com.google.common.io.MoreFiles;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class GridWriter implements Closeable{
	
	private final ByteArrayOutputWrapper baOffsets = new ByteArrayOutputWrapper(1024);
	private final DataOutputStream indexOutput;
	private final CountingOutputStream dataOutput;
	
	
	public GridWriter(String path) throws FileNotFoundException{
		indexOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path+".idx")));
		dataOutput = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
	}
	
	public long write(long zId, int seq, int[] offsets, int size, byte[] data) throws IOException{
		final long pos = dataOutput.getCount();
		
		indexOutput.writeLong(zId);
		indexOutput.writeInt(seq);
		indexOutput.writeLong(pos);
		
		baOffsets.reset();
		baOffsets.writeUInt32(size);
		long offset = 0;
		for(int i=0; i<size; i++){
			offset = baOffsets.writeUInt64Delta(offsets[i], offset);
		}
		final FastByteArrayOutputStream offsetStream = baOffsets.getByteArrayStream();
		
		dataOutput.write(offsetStream.array, 0, offsetStream.length);
		dataOutput.write(data);
		
		return dataOutput.getCount() - pos;
	}

	@Override
	public void close() throws IOException {
		try {
			indexOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			dataOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
