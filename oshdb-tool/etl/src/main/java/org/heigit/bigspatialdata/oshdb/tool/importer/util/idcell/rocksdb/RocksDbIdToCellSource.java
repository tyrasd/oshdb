package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class RocksDbIdToCellSource implements IdToCellSource{
	private final long defaultValue = -1;
	private final RocksDB db;

	private RocksDbIdToCellSource(RocksDB db) {
		this.db = db;
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
	
	public long get(long key) throws IOException {
		try {
			byte[] value = db.get(Longs.toByteArray(key));
			if (value != null)
				return Longs.fromByteArray(value);
			return defaultValue;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public LongSet get(LongSortedSet keys) throws IOException {
		List<byte[]> byteKeys = new ArrayList<>(keys.size());
		keys.forEach((long key) -> byteKeys.add(Longs.toByteArray(key)));
		try {
			LongSet ret = new LongArraySet(keys.size());
			db.multiGet(byteKeys).entrySet().forEach(entry -> {
				ret.add(Longs.fromByteArray(entry.getValue()));
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
