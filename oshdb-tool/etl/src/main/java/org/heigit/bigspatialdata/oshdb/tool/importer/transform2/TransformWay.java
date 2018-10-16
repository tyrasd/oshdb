package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellRefMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSource;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Way;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransformWay extends Transformer {

	private final IdToCellSource nodeToCell;
	private final CellRefMap cellRefMap;

	private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);

	public TransformWay(TagToIdMapper tagToId, CellDataMap cellDataMap, IdToCellSink idToCellSink,
			IdToCellSource nodeToCell, CellRefMap cellRefMap) {
		super(tagToId, cellDataMap, idToCellSink);
		this.nodeToCell = nodeToCell;
		this.cellRefMap = cellRefMap;
	}

	private OSMWay getOSM(Way entity) {
		return new OSMWay(entity.getId() //
				, modifiedVersion(entity) //
				, new OSHDBTimestamp(entity.getTimestamp()) //
				, entity.getChangeset() //
				, entity.getUserId() //
				, getKeyValue(entity.getTags()) //
				, convertNodeIdsToOSMMembers(entity.getRefs()));
	}

	private OSMMember[] convertNodeIdsToOSMMembers(long[] refs) {
		OSMMember[] ret = new OSMMember[refs.length];
		int i = 0;
		for (long ref : refs) {
			ret[i++] = new OSMMember(ref, OSMType.NODE, -1);
		}
		return ret;
	}

	private void storeRef(long cellId, LongSortedSet nodes) throws IOException {
		cellRefMap.add(cellId, nodes, null);
	}

	@Override
	protected OSMType getType() {
		return OSMType.WAY;
	}

	@Override
	protected long transform(long id, OSMType type, List<Entity> versions) throws IOException {
		final List<OSMWay> ways = new ArrayList<>(versions.size());
		final LongSortedSet nodeIds = new LongAVLTreeSet();
		for (Entity version : versions) {
			Way way = (Way) version;
			ways.add(getOSM(way));
			for (long ref : way.refs) {
				nodeIds.add(ref);
			}
		}

		final LongSet cellIds = nodeToCell.get(nodeIds);
		final long cellId = findBestFittingCellId(cellIds);
		final long baseId = 0;

		final TransformOSHWay osh = TransformOSHWay.build(baData, baRecord, baAux, ways, nodeIds, baseId, 0, 0, 0);
		final ByteBuffer record = ByteBuffer.wrap(baRecord.array(), 0, baRecord.length());

		store(cellId, record);
		storeRef(cellId, nodeIds);
		idToCell(id, cellId);

		return id;
	}

	private static void ingestIdToCellMapping(Path workDir, String type) throws IOException {
		String dbFilePath = workDir.resolve("transform_idToCell_" + type).toString();
		List<String> sstFilePaths = Lists.newArrayList();
		Files.newDirectoryStream(workDir, "transform_idToCell_" + type + "_*").forEach(path -> {
			sstFilePaths.add(path.toString());
		});
		if (!sstFilePaths.isEmpty()) {
			try (Options options = new Options()) {
				options.setCreateIfMissing(true);
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L);
				options.setTableFormatConfig(blockTableConfig);
				options.setOptimizeFiltersForHits(true);
				IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
				ingestExternalFileOptions.setMoveFiles(true);
				try (RocksDB db = RocksDB.open(options, dbFilePath)) {
					db.ingestExternalFile(sstFilePaths, ingestExternalFileOptions);
				} catch (RocksDBException e) {
					throw new IOException(e);
				}
			}
		}
	}

	public static void transform(Args args) throws IOException {
		Path workDir = args.workDir;
		Path pbf = args.pbf;

		int workerId = args.worker;
		int workerTotal = args.totalWorkers;

		ingestIdToCellMapping(workDir, "node");

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);
		final long start = pbfMeta.wayStart;
		final long end = pbfMeta.wayEnd;
		final long hardEnd = pbfMeta.wayEnd;

		long chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd = chunkStart;

		if (workerTotal > 1) {
			chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		} else {
			chunkSize = end - start;
		}

		for (int i = 0; i <= workerId; i++) {
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart + chunkSize, end);
		}

		final long availableHeapMemory = SizeEstimator.estimateAvailableMemory();
		final long memDataMap = availableHeapMemory / 4;
		final long memLookUp = availableHeapMemory / 4;

		final Transform transform = Transform.of(pbf, chunkStart, chunkEnd, end, workerId);
		final TagToIdMapper tagToId = TransformerTagRoles.getTagToIdMapper(workDir);
		final String sstFileName = String.format("transform_idToCell_way_%02d.sst", workerId);
		final String sstFilePath = workDir.resolve(sstFileName).toString();
		final String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();
		try (final CellDataMap cellDataMap = new CellDataMap(workDir, String.format("transform_way_%02d", workerId),memDataMap /2);
			 final CellRefMap cellRefMap =   new CellRefMap(workDir, String.format("transform_ref_way_%02d", workerId), memDataMap/2 );
				final IdToCellSink idToCellSink = RocksDbIdToCellSink.open(sstFilePath)) {

			try (final LRUCache lruCache = new LRUCache(memLookUp)) {

				try (IdToCellSource node2Cell = RocksDbIdToCellSource.open(nodeDbFilePath, lruCache)) {
					TransformWay node = new TransformWay(tagToId, cellDataMap, idToCellSink, node2Cell,cellRefMap);
					Stopwatch stopwatch = Stopwatch.createStarted();
					transform.transform(node, () -> {
						System.out.println("complete!");
					});
					System.out.println(stopwatch);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Args config= Transform.parse(args);
		if (config == null)
			return;
		TransformWay.transform(config);
	}

}
