package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellRefMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSource;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;
import org.rocksdb.BlockBasedTableConfig;
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

public class TransformRelation extends Transformer {
	
	private final IdToCellSource nodeToCell;
	private final IdToCellSource wayToCell;
	private final CellRefMap cellRefMap;

	private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);
	
	public TransformRelation(TagToIdMapper tagToId, RoleToIdMapper roleToId, CellDataMap cellDataMap, IdToCellSink idToCellSink,
			IdToCellSource nodeToCell, IdToCellSource wayToCell, CellRefMap cellRefMap) {
		super(tagToId,roleToId,cellDataMap, idToCellSink);
		this.nodeToCell = nodeToCell;
		this.wayToCell = wayToCell;
		this.cellRefMap = cellRefMap;
	}
	
	private OSMRelation getOSM(Relation entity) {
		return new OSMRelation(entity.getId() //
				, modifiedVersion(entity) //
				, new OSHDBTimestamp(entity.getTimestamp()) //
				, entity.getChangeset() //
				, entity.getUserId() //
				, getKeyValue(entity.getTags()) //
				, convertToOSMMembers(entity.getMembers()));
	}

	private OSMMember[] convertToOSMMembers(RelationMember[] members) {
		OSMMember[] ret = new OSMMember[members.length];
		int i = 0;
		for (RelationMember member : members) {
			ret[i++] = new OSMMember(member.memId, OSMType.fromInt(member.type), getRole(member.role));
		}
		return ret;
	}

	private void storeRef(long cellId, LongSortedSet nodes, LongSortedSet ways) throws IOException {
		cellRefMap.add(cellId, nodes, ways);
	}
	
	@Override
	protected OSMType getType() {
		return OSMType.RELATION;
	}

	@Override
	protected long transform(long id, OSMType type, List<Entity> versions) throws IOException {
		List<OSMRelation> entities = new ArrayList<>(versions.size());
		LongSortedSet nodeIds = new LongAVLTreeSet();
		LongSortedSet wayIds = new LongAVLTreeSet();
		for (Entity version : versions) {
			final Relation entity = (Relation) version;
			final OSMRelation osm = getOSM(entity);
			entities.add(osm);

			for (OSMMember member : osm.getMembers()) {
				final OSMType memType = member.getType();
				if (memType == OSMType.NODE) {
					nodeIds.add(member.getId());
					nodeIds.add(member.getId());
				} else if (type == OSMType.WAY) {
					wayIds.add(member.getId());
					wayIds.add(member.getId());
				}
			}
		}
		
		final LongSet cellIds;
		if (!nodeIds.isEmpty()) {
			cellIds = nodeToCell.get(nodeIds);
			cellIds.addAll(wayToCell.get(wayIds));
		} else {
			cellIds = wayToCell.get(wayIds);
		}
		
		final long cellId = findBestFittingCellId(cellIds);
		final long baseId = 0;
		final TransformOSHRelation osh = TransformOSHRelation.build(baData, baRecord, baAux, entities, nodeIds, wayIds, baseId, 0, 0, 0);
		final ByteBuffer record = ByteBuffer.wrap(baRecord.array(), 0, baRecord.length());
		
		store(cellId, record);
		storeRef(cellId, nodeIds,wayIds);
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

		ingestIdToCellMapping(workDir, "way");

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);
		final long start = pbfMeta.relationStart;
		final long end = pbfMeta.relationEnd;
		final long hardEnd = pbfMeta.relationEnd;

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
		final RoleToIdMapper roleToId = TransformerTagRoles.getRoleToIdMapper(workDir);
		final String sstFileName = String.format("transform_idToCell_rel_%02d.sst", workerId);
		final String sstFilePath = workDir.resolve(sstFileName).toString();
		final String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();
		final String wayDbFilePath = workDir.resolve("transform_idToCell_way").toString();
		try (final CellDataMap cellDataMap = new CellDataMap(workDir, String.format("transform_rel_%02d", workerId),memDataMap /2);
			 final CellRefMap cellRefMap =   new CellRefMap(workDir, String.format("transform_ref_rel_%02d", workerId), memDataMap/2 );
				final IdToCellSink idToCellSink = RocksDbIdToCellSink.open(sstFilePath)) {

			try (final LRUCache lruCache = new LRUCache(memLookUp)) {

				try (IdToCellSource node2Cell = RocksDbIdToCellSource.open(nodeDbFilePath, lruCache);
					 IdToCellSource way2Cell = RocksDbIdToCellSource.open(wayDbFilePath, lruCache)) {
					TransformRelation node = new TransformRelation(tagToId, roleToId, cellDataMap, idToCellSink, node2Cell, way2Cell, cellRefMap);
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
		Args config;
		//config = Transform.parse(args);
		config = new Args();

		Path dataDir = Paths.get("/home/rtroilo/data");
		Path pbf = dataDir.resolve("nepal.osh.pbf");
		Path workDir = dataDir.resolve("work/nepal4");

		config.pbf = pbf;
		config.workDir = workDir;
		config.worker = 0;
		config.totalWorkers = 1;

		if (config == null)
			return;
		TransformRelation.transform(config);
	}

}
