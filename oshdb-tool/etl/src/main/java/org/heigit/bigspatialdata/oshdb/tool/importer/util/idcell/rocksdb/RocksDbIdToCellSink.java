package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb;

import java.io.Closeable;
import java.io.IOException;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import com.google.common.primitives.Longs;

public class RocksDbIdToCellSink implements IdToCellSink, Closeable {
	
	private final SstFileWriter sstFileWriter;
	
	private RocksDbIdToCellSink(SstFileWriter sstFileWriter){
		this.sstFileWriter = sstFileWriter;
	}
	
	public static RocksDbIdToCellSink open(String sstFilePath,Options options, EnvOptions envOptions) throws RocksDBException {
		SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);
		sstFileWriter.open(sstFilePath);
		
		return new RocksDbIdToCellSink(sstFileWriter);	
	}

	public void put(long key, long value) throws IOException {
		try {
			sstFileWriter.put(Longs.toByteArray(key), Longs.toByteArray(value));
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public void close() throws IOException {
		try {
			sstFileWriter.finish();
			sstFileWriter.close();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
}
