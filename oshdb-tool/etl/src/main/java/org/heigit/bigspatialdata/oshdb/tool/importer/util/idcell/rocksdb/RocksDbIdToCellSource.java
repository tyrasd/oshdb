package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class RocksDbIdToCellSource implements IdToCellSource {
	private static final int[] longByte = new int[] { 0, 8, 16, 24, 32, 40, 48, 56 };
	private byte[] keyBuffer = new byte[5];

	private final long defaultValue = -1;
	private final RocksDB db;

	private RocksDbIdToCellSource(RocksDB db) {
		this.db = db;
	}

	private static byte[] trim(long v, byte[] b) {
		final int l = b.length - 1;
		for (int i = l; i >= 0; i--) {
			b[l - i] = (byte) (v >> longByte[i]);
		}
		return b;
	}

	private static long untrim(byte[] b, int offset, int length) {
		final int l = length - 1;
		long r = ((long) b[offset + 0]) << longByte[l];
		for (int i = l - 1; i >= 0; i--) {
			r = r | ((long) b[offset + l - i] & 0xff) << longByte[i];
		}
		return r;
	}

	private long valueToLong(byte[] v) {
		// z:0,1,2,3 - 1
		// z:4,5,6,7 - 2
		// z:8,9,10,11 - 3
		// z:12,13,14,15 - 4

		int z = (int) v[0];
		long id;
		if (z > 11) {
			id = untrim(v, 1, 4);
		} else if (z > 7) {
			id = untrim(v, 1, 3);
		} else if (z > 3) {
			id = untrim(v, 1, 2);
		} else {
			id = untrim(v, 1, 1);
		}
		return ZGrid.addZoomToId(id, z);
	}

	public static RocksDbIdToCellSource open(String path, Cache cache) throws IOException {
		try(final Options options = new Options()) {
			BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
			blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB
			blockTableConfig.setCacheIndexAndFilterBlocks(true);
			blockTableConfig.setBlockCache(cache);

			options.setTableFormatConfig(blockTableConfig);
			options.setOptimizeFiltersForHits(true);
			options.optimizeForPointLookup(64L * 1024L * 1024L);
			return open(path,options);
		}
	}
	
	public static RocksDbIdToCellSource open(String path, Options options) throws IOException {
		RocksDB db;
		try {
			db = RocksDB.openReadOnly(options, path);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		return new RocksDbIdToCellSource(db);
	}

	public static RocksDbIdToCellSource open(String path, Options options, List<String> sstPaths) throws IOException {
		List<String> missing = Lists.newArrayList();
		for (String sst : sstPaths) {
			if (sst != null || !Files.exists(Paths.get(sst))) {
				missing.add(sst);
			}
		}
		if (!missing.isEmpty()) {
			throw new FileNotFoundException("some sst are missing! " + Iterables.toString(missing));
		}

		IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
		ingestExternalFileOptions.setMoveFiles(true);
		try (RocksDB db = RocksDB.open(options, path)) {
			db.ingestExternalFile(sstPaths, ingestExternalFileOptions);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}

		return open(path, options);
	}
	
	public RocksDB getDb(){
		return db;
	}

	public long get(long key) throws IOException {
		try {
			byte[] value = db.get(trim(key, keyBuffer));
			if (value != null)
				return valueToLong(value);
			return defaultValue;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public LongSet get(LongSortedSet keys) throws IOException {
		List<byte[]> byteKeys = new ArrayList<>(keys.size());
		keys.forEach((long key) -> byteKeys.add(trim(key, new byte[5])));
		try {
			LongSet ret = new LongArraySet(keys.size());
			Set<Entry<byte[],byte[]>> entries = db.multiGet(byteKeys).entrySet();
			
			entries.forEach(entry -> {
				ret.add(valueToLong(entry.getValue()));
			});
			return ret;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		db.close();
	}
}
