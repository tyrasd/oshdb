package org.heigit.bigspatialdata.oshdb.v0_6.util.backref;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.v0_6.util.IteratorTemplate;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerReader;

import com.google.common.io.MoreFiles;

public class BackRefReader extends IteratorTemplate<RefToBackRefs> implements Closeable {

	private final DeltaIntegerReader intReader = new DeltaIntegerReader("",false);
	
	private final InputStream in;
	private final Path path;
	
	private long ref = 0;
	
	public static BackRefReader of(Path path) throws IOException{
		return new BackRefReader(MoreFiles.asByteSource(path).openBufferedStream(),path);
	}
	
	public BackRefReader(InputStream in, Path path){
		this.in = in;
		this.path = path;
	}
	
	@Override
	protected RefToBackRefs getNext() throws Exception {
		if(in.available() <= 0)
			return null;
		
		ref = SerializationUtils.readVulong(in) + ref;
		int size = (int) SerializationUtils.readVulong(in);
		long[] backRefs = new long[size];
		
		intReader.read(in, reader -> {
			for(int i=0; i< size; i++){
				backRefs[i] = reader.next();
			}
		});
		
		return new RefToBackRefs(ref, backRefs);
	}
	
	@Override
	public void close() throws IOException {
		in.close();
	}
	
}
