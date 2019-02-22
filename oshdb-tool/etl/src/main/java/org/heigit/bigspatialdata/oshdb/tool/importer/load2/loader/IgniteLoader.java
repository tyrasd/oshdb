package org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellBitmaps;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderBitmap;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderCellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader.H2Loader.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.CellId;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

public class IgniteLoader extends GridLoader implements Closeable {

	private final IgniteDataStreamer<Long, GridOSHNodes> nodeStream;
	private final IgniteDataStreamer<Long, GridOSHWays> wayStream;
	private final IgniteDataStreamer<Long, GridOSHRelations> relStream;

	public IgniteLoader(IgniteDataStreamer<Long, GridOSHNodes> nodeStream,
			IgniteDataStreamer<Long, GridOSHWays> wayStream, IgniteDataStreamer<Long, GridOSHRelations> relStream) {
		this.nodeStream = nodeStream;
		this.wayStream = wayStream;
		this.relStream = relStream;
	}

	public static String hRBC(long bytes) {
		final int unit = 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		final String pre = "" + "kMGTPE".charAt(exp - 1);
		return String.format("%.1f,%sB", bytes / Math.pow(unit, exp), pre);
	}

	private long totalBytes = 0;
	private long bytesSinceFlush = 0;
	private long totalNodeBytes = 0;

	@Override
	public void handleNodeGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		super.handleNodeGrid(zId, seq,more, offsets, size, data);

		long bytes = size * 4 + data.length;
		bytesSinceFlush += bytes;
		totalBytes += bytes;
		totalNodeBytes += bytes;

		System.out.printf("n,%2d,%8d,%3d,%4d,%10s,%10s,%10s%n", ZGrid.getZoom(zId),
				ZGrid.getIdWithoutZoom(zId), seq, size, hRBC(bytes), hRBC(totalNodeBytes), hRBC(totalBytes));
	}

	@Override
	public void handleNodeGrid(GridOSHNodes grid, int seq) {
		final int level = grid.getLevel();
		final long id = grid.getId();

		final long levelId = CellId.getLevelId(level, id);
		nodeStream.addData(levelId, grid);

		if (bytesSinceFlush >= 1L * 1024L * 1024L * 1024L) {
			nodeStream.flush();
			bytesSinceFlush = 0;

		}
	}

	private long totalWayBytes = 0;

	@Override
	public void handleWayGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		super.handleWayGrid(zId, seq, more, offsets, size, data);

		long bytes = size * 4 + data.length;
		bytesSinceFlush += bytes;
		totalBytes += bytes;
		totalWayBytes += bytes;

		System.out.printf("w,%2d,%8d,%3d,%4d,%10s,%10s,%10s%n", ZGrid.getZoom(zId),
				ZGrid.getIdWithoutZoom(zId), seq, size, hRBC(bytes), hRBC(totalWayBytes), hRBC(totalBytes));

	}

	@Override
	public void handleWayGrid(GridOSHWays grid, int seq) {
		final int level = grid.getLevel();
		final long id = grid.getId();

		final long levelId = CellId.getLevelId(seq,level, id);
		wayStream.addData(levelId, grid);

		if (bytesSinceFlush >= 1L * 1024L * 1024L * 1024L) {
			wayStream.flush();
			bytesSinceFlush = 0;
		}
	}

	private long totalRelBytes = 0;

	@Override
	public void handleRelationGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		super.handleWayGrid(zId, seq,more, offsets, size, data);

		long bytes = size * 4 + data.length;
		bytesSinceFlush += bytes;
		totalBytes += bytes;
		totalRelBytes += bytes;
		System.out.printf("r,%2d,%8d,%3d,%4d,%10s,%10s,%10s%n", ZGrid.getZoom(zId),ZGrid.getIdWithoutZoom(zId), seq, size, hRBC(bytes), hRBC(totalRelBytes), hRBC(totalBytes));

	}

	@Override
	public void handleRelationGrid(GridOSHRelations grid, int seq) {
		final int level = grid.getLevel();
		final long id = grid.getId();

		final long levelId = CellId.getLevelId(seq,level, id);
		relStream.addData(levelId, grid);

		if (bytesSinceFlush >= 1L * 1024L * 1024L * 1024L) {
			wayStream.flush();
			bytesSinceFlush = 0;
		}

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void missingNode(long id) {
		// TODO Auto-generated method stub
	}

	@Override
	public void missingWay(long id) {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean filterNode(TransformOSHNode osh) {
		return super.filterNode(osh);
	}

	@Override
	public boolean filterWay(TransformOSHWay osh) {
		return super.filterWay(osh);
	}

	@Override
	public boolean filterRelation(TransformOSHRelation osh) {
		return super.filterRelation(osh);
	}

	private static PeekingIterator<CellBitmaps> merge(Path workDir, String glob) throws IOException {
		List<ReaderBitmap> readers = Lists.newArrayList();
		for (Path path : Files.newDirectoryStream(workDir, glob)) {
			readers.add(new ReaderBitmap(path));
		}
		return Iterators.peekingIterator(Iterators.mergeSorted(readers, (a, b) -> {
			int c = ZGrid.ORDER_DFS_TOP_DOWN.compare(a.cellId, b.cellId);
			return c;
		}));
	}

	private static PeekingIterator<CellData> merge(Path workDir, OSMType type, String glob) throws IOException {
		List<ReaderCellData> readers = Lists.newArrayList();
		for (Path path : Files.newDirectoryStream(workDir, glob)) {
			readers.add(new ReaderCellData(path, type));
		}
		return Iterators.peekingIterator(Iterators.mergeSorted(readers, (a, b) -> {
			int c = ZGrid.ORDER_DFS_BOTTOM_UP.compare(a.cellId, b.cellId);
			if (c == 0)
				c = Long.compare(a.id, b.id);
			return c;
		}));
	}

	public static class Args {
		@Parameter(names = { "-workDir",
				"--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = true, order = 10)
		public Path workDir;

		@Parameter(names = { "-ignite", "-igniteConfig",
				"-icfg" }, description = "Path ot ignite-config.xml", required = true, order = 1)
		public File ignitexml;

		@Parameter(names = { "--prefix" }, description = "cache table prefix", required = false)
		public String prefix;
	}

	public static void main(String[] args) throws IOException, IgniteCheckedException {
		Args config = new Args();
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

		final Path workDir = config.workDir;

		PeekingIterator<CellBitmaps> bitmapWayRefReader = merge(workDir, "transform_ref_way_*");
		PeekingIterator<CellBitmaps> bitmapRelRefReader = merge(workDir, "transform_ref_relation_*");

		PeekingIterator<CellData> nodeReader = merge(workDir, OSMType.NODE, "transform_node_*");
		PeekingIterator<CellData> wayReader = merge(workDir, OSMType.WAY, "transform_way_*");
		PeekingIterator<CellData> relReader = merge(workDir, OSMType.RELATION, "transform_relation_*");

		PeekingIterator<CellData> entityReader = Iterators
				.peekingIterator(Iterators.mergeSorted(Lists.newArrayList(nodeReader, wayReader, relReader), (a, b) -> {
					int c = ZGrid.ORDER_DFS_BOTTOM_UP.compare(a.cellId, b.cellId);
					if (c == 0) {
						c = a.type.compareTo(b.type) * -1;
						if (c == 0) {
							c = Long.compare(a.id, b.id);
						}
					}
					return c;
				}));

		Stopwatch stopwatch = Stopwatch.createStarted();
		System.out.println("skipping invalid entities ...");
		while (entityReader.hasNext() && entityReader.peek().cellId == -1) {
			entityReader.next();
		}
		System.out.println(stopwatch);

		final File igniteXML = config.ignitexml;
		final String prefix = config.prefix;

		Ignition.setClientMode(true);
		IgniteConfiguration cfg = IgnitionEx.loadConfiguration(igniteXML.toString()).get1();
		cfg.setIgniteInstanceName("IgniteImportClientInstance");

		try (Ignite ignite = Ignition.start(cfg)) {
			ignite.cluster().active(true);

			try (IgniteDataStreamer<Long, GridOSHNodes> nodeStream = openStreamer(ignite, prefix + "_grid_node");
					IgniteDataStreamer<Long, GridOSHWays> wayStream = openStreamer(ignite, prefix + "_grid_way");
					IgniteDataStreamer<Long, GridOSHRelations> relStream = openStreamer(ignite,
							prefix + "_grid_relation");
					IgniteLoader handler = new IgniteLoader(nodeStream, wayStream, relStream)) {
				LoaderGrid loader = new LoaderGrid(entityReader, bitmapWayRefReader, bitmapRelRefReader, handler);
				System.out.println("start loading ...");
				stopwatch.reset().start();
				loader.run();
				System.out.println(stopwatch);
			} finally {
				// deactive cluster after import, so that all caches get persist
				System.out.println("deactive cluster");
				ignite.cluster().active(false);
			}
		}
	}

	private static <T> IgniteDataStreamer<Long, T> openStreamer(Ignite ignite, String cacheName) {
		CacheConfiguration<Long, T> cacheCfg = new CacheConfiguration<>(cacheName);
		cacheCfg.setBackups(0);
		cacheCfg.setCacheMode(CacheMode.PARTITIONED);
		
		IgniteCache<Long, T> cache = ignite.getOrCreateCache(cacheCfg);
		ignite.cluster().disableWal(cacheName);
		return ignite.dataStreamer(cache.getName());
	}

}
