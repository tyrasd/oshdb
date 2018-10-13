package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellRefMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli.TransformArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSource;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.heigit.bigspatialdata.oshpbf.parser.util.MyLambdaSubscriber;
import org.reactivestreams.Publisher;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;

public class Transform {
	private static final long MB = 1024L * 1024L;
	private static final long GB = 1024L * MB;

	private final long maxMemory;
	private Path workDirectory = Paths.get(".");

	static {
		RocksDB.loadLibrary();
	}

	private Transform(long maxMemory) {
		this.maxMemory = maxMemory;
	}

	public static Transform withMaxMemory(long availableMemory) {
		return new Transform(availableMemory);
	}

	public Transform withWorkDirectory(Path workDirectory) {
		this.workDirectory = workDirectory;
		return this;
	}

	public static TagToIdMapper getTagToIdMapper(Path workDirectory) throws FileNotFoundException, IOException {
		return TransformerTagRoles.getTagToIdMapper(workDirectory);
	}

	public static RoleToIdMapper getRoleToIdMapper(Path workDirectory) throws FileNotFoundException, IOException {
		return TransformerTagRoles.getRoleToIdMapper(workDirectory);
	}

	public void transformNodes(OsmPbfMeta pbfMeta, int maxZoom, TagToIdMapper tag2Id, int workerId, int workerTotal)
			throws IOException {
		
		long start = pbfMeta.nodeStart;
		long end = pbfMeta.nodeEnd;
		long hardEnd = pbfMeta.nodeEnd;
		
		long chunkSize = (long) Math.ceil((double)(end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd= chunkStart;
		for(int i=0; i<=workerId; i++){
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart+chunkSize, end);
		}
		
		long memDataMap = maxMemory;
		CellDataMap cellDataMap = new CellDataMap(workDirectory, String.format("transform_node_%02d", workerId), memDataMap ); 
		final Transformer transformer = new TransformerNode(maxMemory, maxZoom, workDirectory, tag2Id, cellDataMap, null, workerId);
				
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, chunkStart, chunkEnd, hardEnd, workerId != 0) //
				.map(osh -> osh.getVersions());
		RxOshPbfReader.subscribe(flow, transformer::transform, transformer::error, transformer::complete);
	}

	public void transformWays(OsmPbfMeta pbfMeta, int maxZoom, TagToIdMapper tag2Id, IdToCellSource node2cell,
			int workerId, int workerTotal) throws IOException {
		
		long start = pbfMeta.wayStart;
		long end = pbfMeta.wayEnd;
		long hardEnd = pbfMeta.wayEnd;
		
		long chunkSize = (long) Math.ceil((double)(end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd= chunkStart;
		for(int i=0; i<=workerId; i++){
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart+chunkSize, end);
		}
		
		long memRefMap = 1L*GB;
		long memDataMap = maxMemory - memRefMap;
		CellDataMap cellDataMap = new CellDataMap(workDirectory, String.format("transform_way_%02d", workerId), memDataMap );
		CellRefMap cellRefMap = new CellRefMap(workDirectory, String.format("transform_ref_way_%02d", workerId), memRefMap );
		final Transformer transformer = new TransformerWay(maxMemory, maxZoom, workDirectory, tag2Id, node2cell, cellDataMap,cellRefMap, workerId );
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, chunkStart, chunkEnd, hardEnd, workerId != 0) //
				.map(osh -> osh.getVersions());
		RxOshPbfReader.subscribe(flow, transformer::transform, transformer::error, transformer::complete);

	}

	public void transformRelations(OsmPbfMeta pbfMeta, int maxZoom, TagToIdMapper tag2Id, RoleToIdMapper role2Id,
			IdToCellSource node2cell, IdToCellSource way2cell, int workerId, int workerTotal) throws IOException {
		
		long start = pbfMeta.relationStart;
		long end = pbfMeta.relationEnd;
		long hardEnd = pbfMeta.relationEnd;
		
		long chunkSize = (long) Math.ceil((double)(end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd= chunkStart;
		for(int i=0; i<=workerId; i++){
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart+chunkSize, end);
		}
		
		long memRefMap = 1L*GB;
		long memDataMap = maxMemory - memRefMap;
		CellDataMap cellDataMap = new CellDataMap(workDirectory, String.format("transform_node_%02d", workerId), memDataMap );
		CellRefMap cellRefMap = new CellRefMap(workDirectory, String.format("transform_ref_node_%02d", workerId), memRefMap );
		final Transformer transformer = new TransformerRelation(maxMemory, maxZoom, workDirectory, tag2Id, role2Id, node2cell, way2cell, cellDataMap,cellRefMap, workerId);
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, chunkStart, chunkEnd, hardEnd,workerId != 0) //
				.map(osh -> osh.getVersions());
		RxOshPbfReader.subscribe(flow, transformer::transform, transformer::error, transformer::complete);

	}


	
	private static void ingestIdToCellMapping(Path workDir, String type) throws IOException{
		String dbFilePath = workDir.resolve("transform_idToCell_"+type).toString();
		List<String> sstFilePaths = Lists.newArrayList();
		Files.newDirectoryStream(workDir, "transform_idToCell_"+type+"_*").forEach(path -> {
			sstFilePaths.add(path.toString());
		});
		if (!sstFilePaths.isEmpty()) {
			try (Options options = new Options()) {
				options.setCreateIfMissing(true);
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * MB); 
				options.setTableFormatConfig(blockTableConfig);

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

	public static void transform(TransformArgs config) throws Exception {


		final Path pbf = config.pbf;
		final Path workDir = config.common.workDir;

		final String step = config.step;
		final int maxZoom = config.maxZoom;
		boolean overwrite = config.overwrite;

		int worker = config.distribute.worker;
		int workerTotal = config.distribute.totalWorkers;

		if (worker >= workerTotal)
			throw new IllegalArgumentException("worker must be lesser than totalWorker!");

		if (workerTotal > 1 && (step.startsWith("a")))
			throw new IllegalArgumentException(
					"step all with totalWorker > 1 is not allwod use step (node,way or relation)");

		final long availableHeapMemory = SizeEstimator.estimateAvailableMemory();
		long availableMemory = availableHeapMemory - Math.max(1 * GB, availableHeapMemory / 3);

		System.out.println("Transform:");
		System.out.println("avaliable memory: " + availableMemory / 1024L / 1024L + " mb");

		// final Transform transform =
		// Transform.withMaxMemory(availableMemory).withWorkDirectory(workDir);
		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);

		final TagToIdMapper tag2Id = Transform.getTagToIdMapper(workDir);
		availableMemory -= tag2Id.estimatedSize();

		if (step.startsWith("a") || step.startsWith("n")) {
			long maxMemory = availableMemory;
			if (maxMemory < 100 * MB)
				System.out.println(
						"warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
			if (maxMemory < 1 * MB)
				throw new Exception(
						"to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");

			System.out.println("maxMemory for transformation: " + maxMemory / 1024L / 1024L + " mb");
			System.out.println("start transforming nodes ...");
			Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformNodes(pbfMeta, maxZoom, tag2Id,
					worker, workerTotal);

			String type = "node";

			System.out.println(" done!");
		}

		if (step.startsWith("a") || step.startsWith("w")) {
			ingestIdToCellMapping(workDir, "node");

			final long mapMemory = availableMemory / 3L;
			String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();

			try (LRUCache lruCache = new LRUCache(mapMemory); Options options = new Options()) {
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB
				blockTableConfig.setCacheIndexAndFilterBlocks(true);
				blockTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
				blockTableConfig.setBlockCache(lruCache);
				options.setTableFormatConfig(blockTableConfig);

				try (IdToCellSource node2Cell = RocksDbIdToCellSource.open(nodeDbFilePath, options)) {
					long maxMemory = availableMemory - mapMemory;
					if (maxMemory < 100 * MB)
						System.out.println(
								"warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
					if (maxMemory < 1 * MB)
						throw new Exception(
								"to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");

					System.out.println("maxMemory for transformation: " + maxMemory / 1024L / 1024L + " mb");
					System.out.println("start transforming ways ...");
					Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformWays(pbfMeta, maxZoom,
							tag2Id, node2Cell, worker, workerTotal);
				}
			}
			System.out.println(" done!");
		}

		if (step.startsWith("a") || step.startsWith("r")) {
			ingestIdToCellMapping(workDir, "way");
			
			final RoleToIdMapper role2Id = Transform.getRoleToIdMapper(workDir);
			availableMemory -= role2Id.estimatedSize();
			final long mapMemory = availableMemory / 3L;

			String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();
			String wayDbFilePath = workDir.resolve("transform_idToCell_way").toString();
			System.out.println("LRUCache memory "+mapMemory);
			try (LRUCache lruCache = new LRUCache(mapMemory); Options options = new Options()) {
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB

				
				blockTableConfig.setCacheIndexAndFilterBlocks(true);
				blockTableConfig.setPinL0FilterAndIndexBlocksInCache(true);

				blockTableConfig.setBlockCache(lruCache);

				options.setTableFormatConfig(blockTableConfig);
				try (IdToCellSource node2Cell = RocksDbIdToCellSource.open(nodeDbFilePath, options);
					 IdToCellSource way2Cell = RocksDbIdToCellSource.open(wayDbFilePath, options)) {

					long maxMemory = availableMemory - mapMemory;
					if (maxMemory < 100 * MB)
						System.out.println(
								"warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
					if (maxMemory < 1 * MB)
						throw new Exception(
								"to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");

					System.out.println("maxMemory for transformation: " + maxMemory / 1024L / 1024L + " mb");
					System.out.println("start transforming relations ...");
					Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformRelations(pbfMeta, maxZoom,
							tag2Id, role2Id, node2Cell, way2Cell, worker, workerTotal);
				}
			}
			System.out.println(" done!");
		}
	}

	public static void main(String[] args) throws Exception {
		TransformArgs config = new TransformArgs();
		JCommander jcom = JCommander.newBuilder().addObject(config).build();

		try {
			jcom.parse(args);
		} catch (ParameterException e) {
			System.out.println("");
			System.out.println(e.getLocalizedMessage());
			System.out.println("");
			jcom.usage();
			return;
		}
		if (config.common.help) {
			jcom.usage();
			return;
		}
		Stopwatch stopwatch = Stopwatch.createStarted();

		transform(config);
		System.out.println("transform done in " + stopwatch);
	}

}
