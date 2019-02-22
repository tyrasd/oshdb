package org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellBitmaps;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.CellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderBitmap;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.ReaderCellData;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid.Grid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler.OSHDBHandler;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader.H2Loader.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.MoreFiles;

public class SplitDownLoader extends OSHDBHandler implements Closeable {

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

		@Parameter(names = { "--out" }, description = "output path", required = true)
		public Path output;
	}

	public static void main(String[] args) throws IOException {
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
						c = a.type.compareTo(b.type);
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

		final Path outPath = config.output;
		try (SplitDownLoader handler = new SplitDownLoader(outPath)) {
			LoaderGrid loader = new LoaderGrid(entityReader, bitmapWayRefReader, bitmapRelRefReader, handler);
			System.out.println("start loading ...");
			stopwatch.reset().start();
			loader.run();
			System.out.println(stopwatch);
		}
	}

	public static final long MB = 1L * 1024L * 1024L;

	private static final int MAX_ZOOM = 14;
	private static final long MIN_SIZE_MB = 4L * MB;

	private final XYGridTree gridTree = new XYGridTree(14);

	private Map<OSMType, DataOutputStream> indexStreams = new HashMap<>();
	private Map<OSMType, CountingOutputStream> outStreams = new HashMap<>();

	public SplitDownLoader(Path out) throws IOException {
		indexStreams.put(OSMType.NODE,
				new DataOutputStream(MoreFiles.asByteSink(out.resolve("node.index")).openBufferedStream()));
		indexStreams.put(OSMType.WAY,
				new DataOutputStream(MoreFiles.asByteSink(out.resolve("way.index")).openBufferedStream()));
		indexStreams.put(OSMType.RELATION,
				new DataOutputStream(MoreFiles.asByteSink(out.resolve("relation.index")).openBufferedStream()));

		outStreams.put(OSMType.NODE,
				new CountingOutputStream(MoreFiles.asByteSink(out.resolve("node.data")).openBufferedStream()));
		outStreams.put(OSMType.WAY,
				new CountingOutputStream(MoreFiles.asByteSink(out.resolve("way.data")).openBufferedStream()));
		outStreams.put(OSMType.RELATION,
				new CountingOutputStream(MoreFiles.asByteSink(out.resolve("relation.data")).openBufferedStream()));

	}

	@Override
	public boolean loadNodeCondition(Grid grid) {
		if (grid.zoom > MAX_ZOOM)
			return false;
		long size = grid.sizeNodes();
		if (size < MIN_SIZE_MB)
			return false;
		return true;
	}

	@Override
	public boolean loadWayCondition(Grid grid) {
		if (grid.zoom > MAX_ZOOM)
			return false;
		long size = grid.sizeNodes() + grid.sizeRefNodesWay() + grid.sizeWays();
		if (size < MIN_SIZE_MB)
			return false;
		return true;
	}

	@Override
	public boolean loadRelCondition(Grid grid) {
		if (grid.zoom > MAX_ZOOM)
			return false;
		long size = grid.sizeRefRelWays() + grid.sizeRefNodesRel() + grid.sizeRelations();
		if (size < MIN_SIZE_MB)
			return false;
		return true;
	}

	@Override
	public boolean filterNode(TransformOSHNode osh) {
		for (OSMNode osm : osh) {
			if (osm.getRawTags().length > 0)
				return true;
		}
		return false;
	}

	@Override
	public boolean filterWay(TransformOSHWay osh) {
		return true;
	}

	@Override
	public boolean filterRelation(TransformOSHRelation osh) {
		return true;
	}

	@FunctionalInterface
	public static interface GetInstance<T> {
		T apply(byte[] data, int offset, int length, long baseLongitude, long baseLatitude) throws IOException;
	}

	@FunctionalInterface
	public static interface Rebase<T> {
		ByteBuffer apply(T osh, long baseLongitude, long baseLatitude) throws IOException;
	}

	@Override
	public void handleNodeGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data)
			throws IOException {

		split(zId, seq > 0 || more, OSMType.NODE, offsets, size, data,
				(osh, offset, length, lon, lat) -> OSHNode.instance(osh, offset, length, 0, 0, lon, lat),
				(osh, lon, lat) -> {
					List<OSMNode> versions = osh.getVersions();
					return OSHNode.buildRecord(versions, 0, 0, lon, lat);
				});
	}

	@Override
	public void handleWayGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data)
			throws IOException {

		split(zId, seq > 0 || more, OSMType.WAY, offsets, size, data,
				(osh, offset, length, lon, lat) -> OSHWay.instance(osh, offset, length, 0, 0, lon, lat),
				(osh, lon, lat) -> {
					List<OSMWay> versions = osh.getVersions();
					List<OSHNode> nodes = osh.getNodes();
					return OSHWay.buildRecord(versions, nodes, 0, 0, lon, lat);
				});
	}

	@Override
	public void handleRelationGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data)
			throws IOException {

		split(zId, seq > 0 || more, OSMType.RELATION, offsets, size, data,
				(osh, offset, length, lon, lat) -> OSHRelation.instance(osh, offset, length, 0, 0, lon, lat),
				(osh, lon, lat) -> {
					List<OSMRelation> versions = osh.getVersions();
					List<OSHNode> nodes = osh.getNodes();
					List<OSHWay> ways = osh.getWays();
					return OSHRelation.buildRecord(versions, nodes, ways, 0, 0, lon, lat);
				});
	}

	@SuppressWarnings("rawtypes")
	public <T extends OSHEntity> void split(long zId, boolean splitDown, OSMType type, int[] offsets, int size,
			byte[] data, GetInstance<T> getInstance, Rebase<T> rebase) throws IOException {
		final int zoom = ZGrid.getZoom(zId);
		final XYGrid xyGrid = new XYGrid(zoom);
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;

		long xyId = xyGrid.getId(baseLongitude, baseLatitude);

		HashMap<CellId, List<T>> splitDowns = new HashMap<>();
		List<ByteBuffer> buffers = new ArrayList<>();
		
		
		System.out.printf("%s %2d:%8d (%3d) -> %s%n", type, zoom, xyId, size, (splitDown?"split":""));
		
		int pos = 0;
		while (pos < size) {
			int offset = offsets[pos];
			int length = ((pos < size - 1) ? offsets[pos + 1] : data.length) - offset;
			pos++;

			if (!splitDown) {
				buffers.add(ByteBuffer.wrap(data, offset, length));

			} else {
				T osh = getInstance.apply(data, offset, length, baseLongitude, baseLatitude);
				OSHDBBoundingBox oshBBox = osh.getBoundingBox();
				CellId newCell = gridTree.getInsertId(oshBBox);
				splitDowns.computeIfAbsent(newCell, it -> new ArrayList<>()).add(osh);
			}
		}

		if (!splitDown) {
			write(type, xyId, zoom, baseLongitude, baseLatitude, buffers);
		}

		for (Entry<CellId, List<T>> entry : splitDowns.entrySet()) {
			CellId cellId = entry.getKey();
			XYGrid newXyGrid = new XYGrid(cellId.getZoomLevel());
			OSHDBBoundingBox newBbox = newXyGrid.getCellDimensions(cellId.getId());
			long newBaseLongitude = newBbox.getMinLonLong() + (newBbox.getMaxLonLong() - newBbox.getMinLonLong()) / 2;
			long newBaseLatitude = newBbox.getMinLatLong() + (newBbox.getMaxLatLong() - newBbox.getMinLatLong()) / 2;

			buffers.clear();
			List<T> oshs = entry.getValue();
			
			System.out.printf("  -> %2d:%8d (%3d)%n", cellId.getZoomLevel(), cellId.getId(), oshs.size());
			
			for (T osh : oshs) {
				ByteBuffer record = rebase.apply(osh, newBaseLongitude, newBaseLatitude);
				buffers.add(record);
			}
			write(type, cellId.getId(), cellId.getZoomLevel(), newBaseLongitude, newBaseLatitude, buffers);
		}
	}

	public void write(OSMType type, long xyId, int zoom, long baseLongitude, long baseLatitude, List<ByteBuffer> data)
			throws IOException {
		DataOutputStream indexStream = indexStreams.get(type);
		CountingOutputStream outStream = outStreams.get(type);

		indexStream.writeLong(CellId.getLevelId(zoom, xyId));
		indexStream.writeLong(outStream.getCount());

		DataOutputStream out = new DataOutputStream(outStream);
		out.writeLong(xyId);
		out.writeInt(zoom);
		out.writeLong(baseLongitude);
		out.writeLong(baseLatitude);
		out.writeInt(data.size());
		for (ByteBuffer bb : data) {
			out.writeInt(bb.limit());
			out.write(bb.array(), bb.position(), bb.remaining());
		}
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
	public void close() throws IOException {
		for (OutputStream out : outStreams.values()) {
			out.close();
		}

		for (OutputStream out : indexStreams.values()) {
			out.close();
		}

	}

}
