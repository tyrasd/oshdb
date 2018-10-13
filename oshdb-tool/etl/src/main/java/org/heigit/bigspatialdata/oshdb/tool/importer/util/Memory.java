package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class Memory {
	private static final int MAX_BUFFER_SIZE = 1024*1024*1024; // GB
	private static final int[] longByte = new int[] { 0, 8, 16, 24, 32, 40, 48, 56 };
	
	private final long limit;
	private final long bufferSize;
	private final long offsetMask;

	private long size = 0;

	private int index = 0;
	private ByteBuffer buffer;

	private List<ByteBuffer> buffers;

	public Memory(long size) {
		limit = size;
		bufferSize = Math.min(MAX_BUFFER_SIZE, limit);
		offsetMask = bufferSize - 1;

		int numBuffers = (int) (limit / bufferSize);
		if ((numBuffers * bufferSize) < limit)
			numBuffers++;

		buffers = new ArrayList<>(numBuffers);
		// allocate memory
		long total = 0;
		while (total < limit) {
			ByteBuffer bb = ByteBuffer.allocate((int) Math.min(bufferSize, (limit - total)));
			total += bb.capacity();
			buffers.add(bb);
		}

		buffer = buffers.get(0);
	}

	public long pos() {
		return size;
	}

	public long remaining() {
		return limit - size;
	}
	
	public void clear(){
		size = 0;
		buffers.forEach(ByteBuffer::clear);
		index = 0;
		buffer = buffers.get(0);
	}
	
	public void putInt(int value){
		if (buffer.remaining() >= 8)
			buffer.putInt(value);
		else {
			for (int i = 3; i >= 0; i--) {
				if (buffer.remaining() == 0) {
					getNextBuffer();
				}
				buffer.put((byte) (value >> longByte[i]));
			}
		}
		if (!buffer.hasRemaining()) {
			getNextBuffer();
		}
		size += 4;
	}
	
	public int getInt(long pos){
		if (pos > (size - 4))
			throw new IndexOutOfBoundsException("pos: " + pos + " size:" + size);
		int index = getBufferIndex(pos);
		int offset = getBufferOffset(pos);
		ByteBuffer buffer = buffers.get(index);
		if ((offset + 4) < bufferSize) {
			return buffer.getInt(offset);
		}
		int l = ((int) buffer.get(offset++)) << longByte[3];
		for (int b = 2; b >= 0; b--) {
			if (offset >= bufferSize) {
				index++;
				buffer = buffers.get(index);
				offset = 0;
			}
			l = l | ((int) buffer.get(offset++) & 0xff) << longByte[b];
		}

		return l;
	}

	public void putLong(long value) {
		if (buffer.remaining() >= 8)
			buffer.putLong(value);
		else {
			for (int i = 7; i >= 0; i--) {
				if (buffer.remaining() == 0) {
					getNextBuffer();
				}
				buffer.put((byte) (value >> longByte[i]));
			}
		}
		if (!buffer.hasRemaining()) {
			getNextBuffer();
		}
		size += 8;
	}

	public long getLong(long pos) {
		if (pos > (size - 8))
			throw new IndexOutOfBoundsException("pos: " + pos + " size:" + size);
		int index = getBufferIndex(pos);
		int offset = getBufferOffset(pos);
		ByteBuffer buffer = buffers.get(index);
		if ((offset + 8) < bufferSize) {
			return buffer.getLong(offset);
		}
		long l = ((long) buffer.get(offset++)) << longByte[7];
		for (int b = 6; b >= 0; b--) {
			if (offset >= bufferSize) {
				index++;
				buffer = buffers.get(index);
				offset = 0;
			}
			l = l | ((long) buffer.get(offset++) & 0xff) << longByte[b];
		}

		return l;
	}

	public void put(byte[] bytes, int offset, int length) throws IOException {
		if ((size + length) > limit)
			throw new IOException("not enough space left!");

		int written = 0;
		while (written < length) {
			int write = Math.min(length - written, buffer.remaining());
			buffer.put(bytes, offset + written, write);
			written += write;
			if (!buffer.hasRemaining()) {
				getNextBuffer();
			}

		}
		size += written;
	}

	public void get(long pos, byte[] dest, int destOffset, int length) throws IOException {
		if (pos > (size - length))
			throw new IndexOutOfBoundsException("pos: " + pos + " size:" + size+" length:"+length);

		int index = getBufferIndex(pos);
		int offset = getBufferOffset(pos);
		ByteBuffer buffer = buffers.get(index++);
		if ((offset + length) < bufferSize) {
			buffer.get(dest, destOffset, length);
			return;
		}

		while (length > 0) {
			int read = (int) Math.min(bufferSize - offset, length);
			buffer.position(offset);
			buffer.get(dest, destOffset, read);
			buffer = buffers.get(index++);
			offset = 0;
			destOffset += read;
			length -= read;
		}
	}
	
	public void write(long pos, int length, OutputStream out) throws IOException {
		if (pos > (size - length))
			throw new IndexOutOfBoundsException("pos: " + pos + " size:" + size);
		
		int index = getBufferIndex(pos);
		int offset = getBufferOffset(pos);
		ByteBuffer buffer = buffers.get(index++);
		while(length > 0){
			int read = (int) Math.min(bufferSize - offset, length);
			buffer.position(offset);
			out.write(buffer.array(), offset, read);
			buffer = buffers.get(index++);
			offset = 0;
			length -= read;
		}
	}

	

	private int getBufferIndex(long pos) {
		return (int) (pos / bufferSize);
	}

	private int getBufferOffset(long pos) {
		return (int) (pos & offsetMask);
	}

	private ByteBuffer getNextBuffer() {
		index++;
		buffer = buffers.get(index);
		return buffer;
	}

	@Override
	public String toString() {
		return "Memory [limit=" + limit + ", bufferSize=" + bufferSize + ", pos=" + size + ", size=" + size
				+ ", buffers=" + buffers.size() + "]";
	}
	
	public static void main(String[] args) {
		Memory memory = new Memory(1024);
		System.out.println(memory);
		
		LongArrayList offsets = new LongArrayList();
		for(int i=0; i< 10; i++){
			offsets.add(memory.pos());
			memory.putInt(i);
		}
		
		offsets.forEach((long o) -> {
			System.out.println(o +":"+ memory.getInt(o));
		});
		
	}
	
	
}
