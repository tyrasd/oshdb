package org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.TableNames;
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
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.Load.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.Load.LoadHandler;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid.Grid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class H2Loader extends GridLoader implements Closeable {
	public static String hRBC(long bytes) {
		final int unit = 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		final String pre = "" + "kMGTPE".charAt(exp - 1);
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	private final Connection conn;
	private final PreparedStatement insertNode;
	private final PreparedStatement insertWay;
	private final PreparedStatement insertRelation;

	private FastByteArrayOutputStream out = new FastByteArrayOutputStream(1024);

	public H2Loader(String path) throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:" + path + "", "sa", "");
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("drop table if exists " + TableNames.T_NODES.toString()
						+ "; create table if not exists " + TableNames.T_NODES.toString()
						+ "(level int, id bigint, seq int, data blob,  primary key(level,id,seq))");
				insertNode = conn.prepareStatement(
						"insert into " + TableNames.T_NODES.toString() + " (level,id,seq,data) values(?,?,?,?)");

				stmt.executeUpdate("drop table if exists " + TableNames.T_WAYS.toString()
						+ "; create table if not exists " + TableNames.T_WAYS.toString()
						+ "(level int, id bigint, seq int, data blob,  primary key(level,id,seq))");
				insertWay = conn.prepareStatement(
						"insert into " + TableNames.T_WAYS.toString() + " (level,id,seq,data) values(?,?,?,?)");

				stmt.executeUpdate("drop table if exists " + TableNames.T_RELATIONS.toString()
						+ "; create table if not exists " + TableNames.T_RELATIONS.toString()
						+ "(level int, id bigint, seq int, data blob,  primary key(level,id,seq))");
				insertRelation = conn.prepareStatement(
						"insert into " + TableNames.T_RELATIONS.toString() + " (level,id,seq,data) values(?,?,?,?)");
			}
	}

	@Override
	public void handleNodeGrid(GridOSHNodes grid, int seq) {
		try {
			out.reset();
			try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
				oos.writeObject(grid);
				oos.flush();
			}
			FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);
			insertNode.setInt(1, grid.getLevel());
			insertNode.setLong(2, grid.getId());
			insertNode.setInt(3, seq);
			insertNode.setBinaryStream(4, in);
			insertNode.executeUpdate();

			System.out.printf("n %2d:%8d (%3d) -> b:%10s%n", grid.getLevel(), grid.getId(), seq, hRBC(out.length));
		} catch (IOException | SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void handleWayGrid(GridOSHWays grid, int seq) {
		try {
			out.reset();
			try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
				oos.writeObject(grid);
				oos.flush();
			}
			FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);

			insertWay.setInt(1, grid.getLevel());
			insertWay.setLong(2, grid.getId());
			insertWay.setInt(3, seq);
			insertWay.setBinaryStream(4, in);
			insertWay.executeUpdate();

			System.out.printf("w %2d:%8d (%3d) -> b:%10s%n", grid.getLevel(), grid.getId(), seq, hRBC(out.length));
		} catch (IOException | SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void handleRelationGrid(GridOSHRelations grid, int seq) {
		try {
			out.reset();
			try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
				oos.writeObject(grid);
				oos.flush();
			}
			FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);

			insertRelation.setInt(1, grid.getLevel());
			insertRelation.setLong(2, grid.getId());
			insertRelation.setInt(3, seq);
			insertRelation.setBinaryStream(4, in);
			insertRelation.executeUpdate();

			System.out.printf("r %2d:%8d (%3d) -> b:%10s%n", grid.getLevel(), grid.getId(), seq, hRBC(out.length));
		} catch (IOException | SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void close() throws IOException {
		try {
			insertNode.close();
		} catch (SQLException e) {
		}
		try {
			insertWay.close();
		} catch (SQLException e) {
		}
		try {
			insertRelation.close();
		} catch (SQLException e) {
		}
		
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
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

		final String oshdbPath = config.output.toString();
		try (H2Loader handler = new H2Loader(oshdbPath)) {
			LoaderGrid loader = new LoaderGrid(entityReader, bitmapWayRefReader,bitmapRelRefReader, handler);
			System.out.println("start loading ...");
			stopwatch.reset().start();
			loader.run();
			System.out.println(stopwatch);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
