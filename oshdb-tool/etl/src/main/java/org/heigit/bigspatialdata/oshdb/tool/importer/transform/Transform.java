package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli.TransformArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSource;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.SortedLong2LongMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive.MyLambdaSubscriber;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
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
import com.google.common.collect.Multiset.Entry;
import com.google.common.primitives.Longs;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class Transform {

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
		final Transformer transformer = new TransformerNode(maxMemory, maxZoom, workDirectory, tag2Id, workerId);
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, pbfMeta.nodeStart, pbfMeta.nodeEnd, pbfMeta.nodeEnd) //
				.map(osh -> osh.getVersions());
		subscribe(flow, transformer::transform, transformer::error, transformer::complete);
	}

	public void transformWays(OsmPbfMeta pbfMeta, int maxZoom, TagToIdMapper tag2Id, IdToCellSource node2cell,
			int workerId, int workerTotal) throws IOException {
		final Transformer transformer = new TransformerWay(maxMemory, maxZoom, workDirectory, tag2Id, node2cell,
				workerId);
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, pbfMeta.wayStart, pbfMeta.wayEnd, pbfMeta.wayEnd) //
				.map(osh -> osh.getVersions());
		subscribe(flow, transformer::transform, transformer::error, transformer::complete);

	}

	public void transformRelations(OsmPbfMeta pbfMeta, int maxZoom, TagToIdMapper tag2Id, RoleToIdMapper role2Id,
			IdToCellSource node2cell, IdToCellSource way2cell, int workerId, int workerTotal)
			throws IOException {
		final Transformer transformer = new TransformerRelation(maxMemory, maxZoom, workDirectory, tag2Id, role2Id,
				node2cell, way2cell, workerId);
		Flowable<List<Entity>> flow = RxOshPbfReader //
				.readOsh(pbfMeta.pbf, pbfMeta.relationStart, pbfMeta.relationEnd, pbfMeta.relationEnd) //
				.map(osh -> osh.getVersions());
		subscribe(flow, transformer::transform, transformer::error, transformer::complete);

	}

	private static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
			final Consumer<? super Throwable> onError, final Action onComplete) {
		ObjectHelper.requireNonNull(onNext, "onNext is null");
		ObjectHelper.requireNonNull(onError, "onError is null");
		ObjectHelper.requireNonNull(onComplete, "onComplete is null");
		FlowableBlockingSubscribe.subscribe(o, new MyLambdaSubscriber<T>(onNext, onError, onComplete, 1L));
	}

	public static void transform(TransformArgs config) throws Exception {

		final long MB = 1024L * 1024L;
		final long GB = 1024L * MB;

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
		long availableMemory = availableHeapMemory - Math.max(1 * GB, availableHeapMemory / 3); // reserve
																								// at
																								// least
																								// 1GB
																								// or
																								// 1/3
																								// of
																								// the
																								// total
																								// memory

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
			Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformNodes(pbfMeta, maxZoom, tag2Id, worker, workerTotal);
			
			String type = "node";
			String dbFilePath = workDir.resolve("transform_idToCell_"+type).toString();
			List<String> sstFilePaths = Lists.newArrayList();
			Files.newDirectoryStream(workDir, "transform_idToCell_"+type+"_*").forEach(path -> {
				sstFilePaths.add(path.toString());
			});
			
			try ( Options options = new Options()) {
				options.setCreateIfMissing(true);
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB
				options.setTableFormatConfig(blockTableConfig);
				
				IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
				ingestExternalFileOptions.setMoveFiles(true);
				try (RocksDB db = RocksDB.open(options,dbFilePath)) {
					db.ingestExternalFile(sstFilePaths, ingestExternalFileOptions);
				} catch (RocksDBException e) {
					throw new IOException(e);
				}
			}
			
			System.out.println(" done!");
		}

		if (step.startsWith("a") || step.startsWith("w")) {
			final long mapMemory = availableMemory / 3L;
			String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();
			
			try (LRUCache lruCache = new LRUCache(mapMemory); Options options = new Options()) {
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB

				blockTableConfig.setCacheIndexAndFilterBlocks(true);
				blockTableConfig.setPinL0FilterAndIndexBlocksInCache(true);

				blockTableConfig.setBlockCache(lruCache);

				options.setTableFormatConfig(blockTableConfig);

				try (IdToCellSource node2Cell = RocksDbIdToCellSource
						.open(nodeDbFilePath, options)) {
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
			
			String type = "way";
			String dbFilePath = workDir.resolve("transform_idToCell_"+type).toString();
			List<String> sstFilePaths = Lists.newArrayList();
			Files.newDirectoryStream(workDir, "transform_idToCell_"+type+"_*").forEach(path -> {
				sstFilePaths.add(path.toString());
			});
			
			try ( Options options = new Options()) {
				options.setCreateIfMissing(true);
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB
				options.setTableFormatConfig(blockTableConfig);
				
				IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
				ingestExternalFileOptions.setMoveFiles(true);
				try (RocksDB db = RocksDB.open(options,dbFilePath)) {
					db.ingestExternalFile(sstFilePaths, ingestExternalFileOptions);
				} catch (RocksDBException e) {
					throw new IOException(e);
				}
			}
			
			System.out.println(" done!");
		}

		if (step.startsWith("a") || step.startsWith("r")) {
			final RoleToIdMapper role2Id = Transform.getRoleToIdMapper(workDir);
			availableMemory -= role2Id.estimatedSize();
			final long mapMemory = availableMemory / 3L;

			
			String nodeDbFilePath = workDir.resolve("transform_idToCell_node").toString();
			String wayDbFilePath = workDir.resolve("transform_idToCell_way").toString();
			try (LRUCache lruCache = new LRUCache(mapMemory); Options options = new Options()) {
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB

				blockTableConfig.setCacheIndexAndFilterBlocks(true);
				blockTableConfig.setPinL0FilterAndIndexBlocksInCache(true);

				blockTableConfig.setBlockCache(lruCache);

				options.setTableFormatConfig(blockTableConfig);
				try(IdToCellSource node2Cell = RocksDbIdToCellSource.open(nodeDbFilePath, options);
					IdToCellSource way2Cell = RocksDbIdToCellSource.open(wayDbFilePath, options)){

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
				} finally {

				}
			}
			
			String type = "relation";
			String dbFilePath = workDir.resolve("transform_idToCell_"+type).toString();
			List<String> sstFilePaths = Lists.newArrayList();
			Files.newDirectoryStream(workDir, "transform_idToCell_"+type+"_*").forEach(path -> {
				sstFilePaths.add(path.toString());
			});
			
			try ( Options options = new Options()) {
				options.setCreateIfMissing(true);
				BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
				blockTableConfig.setBlockSize(1L * 1024L * 1024L); // 1MB
				options.setTableFormatConfig(blockTableConfig);
				
				IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
				ingestExternalFileOptions.setMoveFiles(true);
				try (RocksDB db = RocksDB.open(options,dbFilePath)) {
					db.ingestExternalFile(sstFilePaths, ingestExternalFileOptions);
				} catch (RocksDBException e) {
					throw new IOException(e);
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
