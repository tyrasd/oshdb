package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class ByteBufferBackedInputStream extends InputStream {
	private ByteBuffer buf;
	
	
	public static  ByteBufferBackedInputStream of(ByteBuffer buf){
		ByteBufferBackedInputStream b = new ByteBufferBackedInputStream();
		b.set(buf);
		return b;
	}
	
	public void set(ByteBuffer buf) {
		this.buf = buf;
	}

	public int read() throws IOException {
		if (!buf.hasRemaining()) {
			return -1;
		}			
		return buf.get()& 0xFF;
	}

	public int read(byte[] bytes, int off, int len) throws IOException {
		len = Math.min(len, buf.remaining());
		buf.get(bytes, off, len);
		return len;
	}
	
	public long readLong(){
		return buf.getLong();
	}
	
	
	 
	public static void main(String[] args) throws IOException {
		FastByteArrayOutputStream out = new FastByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(out);
		
		dout.writeInt(123);
		dout.writeLong(1234L);
		
		ByteBuffer buffer = ByteBuffer.wrap(out.array, 0, out.length);
		
		System.out.println(buffer.getInt());
		System.out.println(buffer.getLong());
		
		
	}
	
	
	
}
