package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

import com.google.common.io.MoreFiles;

public class ReaderCellData implements Iterator<CellData>, Closeable{

	private CellData next = null;
	private Exception exception = null;
	
	private final Path path;
	private final DataInputStream data;
	private final OSMType type;
	private long zId = -2;
	private int size = 0;
	
	public ReaderCellData(Path path, OSMType type) throws IOException{
		this.path = path;
		this.data = new DataInputStream(MoreFiles.asByteSource(path).openBufferedStream());
		this.type = type;
	}
	
	@Override
	public boolean hasNext() {
		try {
			return (next != null) || ((next = getNext()) != null);
		} catch (IOException e) {
			this.exception =e;
		}
		return false;
	}

	@Override
	public CellData next() {
		if(!hasNext())
			throw new NoSuchElementException();
		CellData ret = next;
		next = null;
		return ret;
	}
	
	@Override
	public void close() throws IOException {
		if(data != null)
			data.close();
	}
	
	public boolean hasException(){
		return exception != null;
	}
	
	public Exception getException(){
		return exception;
	}
	
	private CellData getNext() throws IOException{
		while(size == 0 && data.available() > 0){
			zId = data.readLong();
			size = data.readInt();
			int rawSizeInBytes = data.readInt();
		}
		
		if(size > 0 && data.available() > 0){
			final int length = data.readInt();
			final byte[] bytes = new byte[length];
			data.readFully(bytes);
			final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(bytes, 0, bytes.length);
			final long id = wrapper.readUInt64();
			size--;
			return new CellData(zId, id,type, bytes,path);
		}
		return null;
	}

}
