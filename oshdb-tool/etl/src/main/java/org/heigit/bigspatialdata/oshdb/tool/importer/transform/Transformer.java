package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellRefMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import it.unimi.dsi.fastutil.longs.LongSet;

public abstract class Transformer {
	
	static {
		RocksDB.loadLibrary();
	}
	
	private final TagToIdMapper tagToIdMapper;
	private final RoleToIdMapper roleToIdMapper;
	
	private final IdToCellSink idToCellSink;

	protected final Path workDirectory;
	private final int workerId;

	private final ZGrid grid;
	
	private final CellDataMap cellDataMap;
	private final CellRefMap cellRefMap;
	
	

	public Transformer(long maxMemoryUsage, int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper,CellDataMap cellDataMap, CellRefMap cellRefMap, int workerId)
			throws IOException {
		this(maxMemoryUsage, maxZoom, workDirectory, tagToIdMapper, null, cellDataMap, cellRefMap, workerId);
	}
	
	public Transformer(long maxMemoryUsage, int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper, RoleToIdMapper roleToIdMapper, CellDataMap cellDataMap, CellRefMap cellRefMap, int workerId) throws IOException {
		this.workDirectory = workDirectory;
		this.tagToIdMapper = tagToIdMapper;
		this.roleToIdMapper = roleToIdMapper;
		this.workerId = workerId;
		this.grid = new ZGrid(maxZoom);
		
		this.cellDataMap = cellDataMap;
		this.cellRefMap = cellRefMap;
		
		
			
		try(Options options = new Options();
			EnvOptions envOptions = new EnvOptions()){
			// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
			BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
			blockTableConfig.setBlockSize(1024L*1024L*1L); // 1MB
			blockTableConfig.setCacheIndexAndFilterBlocks(true);
			options.setTableFormatConfig(blockTableConfig);
			options.setWriteBufferSize(100L*1024*1024);
			
			String sstFileName = String.format("transform_idToCell_%s_%02d.sst", type().toString().toLowerCase(),workerId);
			String sstFilePath = workDirectory.resolve(sstFileName).toString();
			this.idToCellSink = RocksDbIdToCellSink.open(sstFilePath, options, envOptions);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		
		System.out.println(cellDataMap);
	}

	public void transform(List<Entity> versions) {
		final Entity e = versions.get(0);
		if (type() == e.getType())
			transform(e.getId(), versions);
	}

	public void error(Throwable t) {
		t.printStackTrace();
	}

	public void complete() {
		System.out.println("COMPLETE");
		try {
			idToCellSink.close();
			cellDataMap.close();
			if(cellRefMap != null)
				cellRefMap.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int modifiedVersion(Entity entity) {
		return entity.getVersion() * (entity.isVisible() ? 1 : -1);
	}

	public int[] getKeyValue(TagText[] tags) {
		if (tags.length == 0)
			return new int[0];

		final List<TagId> ids = new ArrayList<>(tags.length);

		for (TagText tag : tags) {
			final int key = tagToIdMapper.getKey(tag.key);
			final int value = tagToIdMapper.getValue(key, tag.value);
			ids.add(TagId.of(key, value));
		}

		ids.sort((a, b) -> {
			final int c = Integer.compare(a.key, b.key);
			return (c != 0) ? c : Integer.compare(a.value, b.value);
		});
		final int[] ret = new int[tags.length * 2];
		int i = 0;
		for (TagId tag : ids) {
			ret[i++] = tag.key;
			ret[i++] = tag.value;
		}

		return ret;
	}

	public int getRole(String role) {
		return (roleToIdMapper != null) ? roleToIdMapper.getRole(role) : 0;
	}

	protected long getCell(long longitude, long latitude) {
		return grid.getIdSingleZIdWithZoom(longitude, latitude);
	}

	protected OSHDBBoundingBox getCellBounce(long cellId) {
		return ZGrid.getBoundingBox(cellId);
	}

	protected static long findBestFittingCellId(Set<Long> cellIds) {
		if (cellIds.isEmpty())
			return -1;

		if (cellIds.size() == 1)
			return cellIds.iterator().next();

		int minZoom = Integer.MAX_VALUE;
		for (Long cellId : cellIds) {
			minZoom = Math.min(minZoom, ZGrid.getZoom(cellId));
		}
		final int zoom = minZoom;
		// bring all to the same zoom level
		Set<Long> bestCellId = cellIds.stream().filter(id -> id >= 0).map(id -> ZGrid.getParent(id, zoom))
				.collect(Collectors.toSet());

		while (bestCellId.size() > 1) {
			cellIds = bestCellId;
			bestCellId = cellIds.stream().map(id -> ZGrid.getParent(id)).collect(Collectors.toSet());
		}
		final long cellId = bestCellId.iterator().next();
		return cellId;
	}

	protected void addIdToCell(long id, long cellId) throws IOException {
		idToCellSink.put(id, cellId);
	}

	protected void store(long cellId, long id, LongFunction<ByteBuffer> data) {
		store(cellId,id,data,null,null,null);
	}

	protected void store(long cellId, long id, LongFunction<ByteBuffer> data, LongSet nodes) {
		store(cellId, id, data, nodes, null, null);
	}

	protected void store(long cellId, long id, LongFunction<ByteBuffer> data, LongSet nodes, LongSet ways) {
		store(cellId, id, data, nodes,ways, null);
	}

	protected void store(long cellId, long id, LongFunction<ByteBuffer> data, LongSet nodes, LongSet ways, LongSet relation) {
		try {
			cellDataMap.add(cellId, id,data);
			if(cellRefMap != null && (nodes != null || ways != null)){
				cellRefMap.add(cellId, nodes, ways);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public abstract void transform(long id, List<Entity> versions);

	public abstract OSMType type();
}
