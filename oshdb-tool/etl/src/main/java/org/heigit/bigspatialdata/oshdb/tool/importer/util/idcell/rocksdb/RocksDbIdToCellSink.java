package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import com.google.common.primitives.Longs;

public class RocksDbIdToCellSink implements IdToCellSink, Closeable {
	private static final int[] longByte = new int[] { 0, 8, 16, 24, 32, 40, 48, 56 };
	
	private final SstFileWriter sstFileWriter;
	private byte[] keyBuffer = new byte[5];
	private byte[][] valBuffers = new byte[][]{
			new byte[2],new byte[3],new byte[4], new byte[5]
	};
	
	public static byte[] trim(long value, byte[] b){
		final int l = b.length -1;
		for(int i=l; i>=0; i--){
			b[l-i] = (byte) (value >> longByte[i]);
		}
		return b;
	}
	
	private RocksDbIdToCellSink(SstFileWriter sstFileWriter){
		this.sstFileWriter = sstFileWriter;
	}
	
	
	public static RocksDbIdToCellSink open(String sstFilePath) throws IOException {
		try(final Options options = new Options(); EnvOptions envOptions = new EnvOptions()) {
			BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
			blockTableConfig.setBlockSize(1024L * 1024L * 1L); // 1MB
			options.setTableFormatConfig(blockTableConfig);
			options.setWriteBufferSize(1024L * 1024L * 128L); // 128MB
			options.setOptimizeFiltersForHits(true);
			return open(sstFilePath, options, envOptions);
		}
	}
	public static RocksDbIdToCellSink open(String sstFilePath,Options options, EnvOptions envOptions) throws IOException {
		SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);
		try {
			sstFileWriter.open(sstFilePath);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		
		return new RocksDbIdToCellSink(sstFileWriter);	
	}
	
	long prevKey = -1;
	byte[] prevKeyBuffer = new byte[5];
	long prevValue = -1;
	boolean first = true;
	public void put(long key, long value) throws IOException {
		if(first){
			System.out.println("First ID in sstfile "+key);
			first = false;
		}

		byte[] valBuffer = null;
		try {
			
			//z:0,1,2,3 - 1
			//z:4,5,6,7 - 2
			//z:8,9,10,11 - 3
			//z:12,13,14,15 - 4
			
			int z = ZGrid.getZoom(value);
			long id = ZGrid.getIdWithoutZoom(value);
			
			if(z > 11){
				valBuffer = trim(id,valBuffers[3]);
			}else if(z > 7){
				valBuffer = trim(id,valBuffers[2]);
			}else if(z > 3){
				valBuffer = trim(id,valBuffers[1]);
			}else{
				valBuffer = trim(id,valBuffers[0]);
			}
			valBuffer[0] = (byte) z;
			sstFileWriter.put(trim(key,keyBuffer), valBuffer);
			prevKey = key;
			byte[] swap = prevKeyBuffer;
			prevKeyBuffer = keyBuffer;
			keyBuffer = swap;
			prevValue = value;
		} catch (RocksDBException e) {
			System.err.printf("key:%d[%s] value:%d[%s], prev= %d[%s],%d%n", key, Arrays.toString(keyBuffer), value, Arrays.toString(valBuffer), prevKey, Arrays.toString(prevKeyBuffer), prevValue);
			throw new IOException(e);
		}
	}
	
	@Override
	public void close() throws IOException {
		System.out.println("last id in sst "+prevKey);
		try {
			sstFileWriter.finish();
			sstFileWriter.close();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
}
