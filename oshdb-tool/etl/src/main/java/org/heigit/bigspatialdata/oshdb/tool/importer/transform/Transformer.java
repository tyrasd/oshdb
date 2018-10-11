package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public abstract class Transformer {
	private static class OSHDataContainer {
		private long sizeInBytesOfData = 0;
		private long estimatedMemoryUsage = SizeEstimator.objOverhead() + 2 * SizeEstimator.intField()
				+ SizeEstimator.linkedList();

		private long lastId = 0;
		private List<byte[]> list = new LinkedList<>();

		public OSHDataContainer add(byte[] data) {
			sizeInBytesOfData += data.length + 4; // count of bytes + 4 bytes
													// for the length of this
													// array
			estimatedMemoryUsage += SizeEstimator.estimatedSizeOf(data) + SizeEstimator.linkedListEntry();
			list.add(data);
			return this;
		}
	}

	private final TagToIdMapper tagToIdMapper;
	private final RoleToIdMapper roleToIdMapper;
	private final Long2ObjectAVLTreeMap<OSHDataContainer> collector;

	private final IdToCellSink idToCellSink;

	private final Map<OSMType, Long2ObjectMap<Roaring64NavigableMap>> typeRefsMaps = new HashMap<>(
			OSMType.values().length);
	private long estimatedMemoryUsage;
	private final long maxMemoryUsage;

	protected final Path workDirectory;
	private final int workerId;
	private int fileNumber = 0;

	private final ZGrid grid;

	public Transformer(long maxMemoryUsage, int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper, int workerId)
			throws IOException {
		this(maxMemoryUsage, maxZoom, workDirectory, tagToIdMapper, null, workerId);
	}
	
	static {
		RocksDB.loadLibrary();
	}

	public Transformer(long maxMemoryUsage, int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper,
			RoleToIdMapper roleToIdMapper, int workerId) throws IOException {
		this.maxMemoryUsage = maxMemoryUsage;
		this.workDirectory = workDirectory;
		this.tagToIdMapper = tagToIdMapper;
		this.roleToIdMapper = roleToIdMapper;
		this.workerId = workerId;
		this.collector = new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN);
		this.grid = new ZGrid(maxZoom);
	
		try(Options options = new Options();
			EnvOptions envOptions = new EnvOptions()){
			// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
			BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
			blockTableConfig.setBlockSize(1024L*1024L*1L); // 1MB
			blockTableConfig.setCacheIndexAndFilterBlocks(true);
			options.setTableFormatConfig(blockTableConfig);
			String sstFileName = String.format("transform_idToCell_%s_%02d.sst", type().toString().toLowerCase(),workerId);
			String sstFilePath = workDirectory.resolve(sstFileName).toString();
			this.idToCellSink = RocksDbIdToCellSink.open(sstFilePath, options, envOptions);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
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
		saveToDisk();
		try {
			idToCellSink.close();
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
		final int cellIdZoom = ZGrid.getZoom(cellId);
		return cellId;
	}

	protected void addIdToCell(long id, long cellId) throws IOException {
		idToCellSink.put(id, cellId);
	}

	private void saveToDisk() {
		if (collector.isEmpty())
			return;
		final String fileName = String.format("transform_%s_%02d_%02d", type().toString().toLowerCase(), workerId, fileNumber);
		final Path filePath = workDirectory.resolve(fileName);
		System.out.print("transformer saveToDisk " + filePath.toString() + " ... ");
		long bytesWritten = 0;

		try (DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(filePath.toFile())))) {
			ObjectIterator<Entry<OSHDataContainer>> iter = collector.long2ObjectEntrySet().iterator();

			while (iter.hasNext()) {
				Entry<OSHDataContainer> entry = iter.next();

				final long cellId = entry.getLongKey();
				final OSHDataContainer container = entry.getValue();
				final long rawSizeLong = container.sizeInBytesOfData;

				final int rawSize = (int) container.sizeInBytesOfData;
				if (rawSize < 0) {
					System.err.println("rawSize is " + rawSize + " longRawSize: " + rawSizeLong);
					System.err.println("cellId " + cellId + " entities:" + container.list.size());
				}

				out.writeLong(cellId);
				out.writeInt(container.list.size());
				out.writeInt(rawSize);
				bytesWritten += 8 + 4 + 4;

				for (byte[] data : container.list) {
					out.writeInt(data.length);
					out.write(data);
					bytesWritten += 4 + data.length;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("done! " + bytesWritten + " bytes");

		fileNumber++;
		collector.clear();
		typeRefsMaps.clear();
		estimatedMemoryUsage = 0L;
	}

	protected void store(long cellId, long id, LongFunction<byte[]> data) {
		OSHDataContainer dataContainer = collector.get(cellId);
		if (dataContainer == null) {
			dataContainer = new OSHDataContainer();
			collector.put(cellId, dataContainer);
			estimatedMemoryUsage += SizeEstimator.avlTreeEntry();
		}
		estimatedMemoryUsage -= dataContainer.estimatedMemoryUsage;

		dataContainer.add(data.apply(dataContainer.lastId));
		dataContainer.lastId = id;
		estimatedMemoryUsage += dataContainer.estimatedMemoryUsage;

		if (estimatedMemoryUsage >= maxMemoryUsage || dataContainer.sizeInBytesOfData >= 1024L * 1024L * 1024L) {
			saveToDisk();
		}
	}

	protected void store(long cellId, long id, LongFunction<byte[]> data, LongSet nodes) {
		store(cellId, id, data);
	}

	protected void store(long cellId, long id, LongFunction<byte[]> data, LongSet nodes, LongSet ways) {
		store(cellId, id, data, nodes);
	}

	protected void store(long cellId, long id, LongFunction<byte[]> data, LongSet nodes, LongSet ways, LongSet relation) {
		store(cellId, id, data, nodes, ways);
	}

	public abstract void transform(long id, List<Entity> versions);

	public abstract OSMType type();
}
