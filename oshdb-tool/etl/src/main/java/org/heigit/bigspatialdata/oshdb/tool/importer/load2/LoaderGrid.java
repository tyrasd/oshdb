package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler.Handler;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDBMember;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.google.common.collect.PeekingIterator;

import okio.Buffer;
import okio.BufferedSource;

public class LoaderGrid {

	public static class Grid {
		long cellId;

		Buffer nodes = new Buffer();
		Buffer ways = new Buffer();
		Buffer rels = new Buffer();

		int cNodes, cRefNodes;
		int cWays, cRefWays;
		int cRels, cRefRels;

		Roaring64NavigableMap refNodes = Roaring64NavigableMap.bitmapOf();
		Roaring64NavigableMap refWays = Roaring64NavigableMap.bitmapOf();

		Buffer refNodesBuffer = new Buffer();
		Buffer refWaysBuffer = new Buffer();

		public int countNodes() {
			return cNodes;
		}

		public long sizeNodes() {
			return nodes.size();
		}

		public long sizeRefNodes() {
			return refNodesBuffer.size();
		}

		public int countWays() {
			return cWays;
		}

		public long sizeWays() {
			return ways.size();
		}

		public long sizeRefWays() {
			return refWaysBuffer.size();
		}

		public int countRelations() {
			return cRels;
		}

		public long sizeRelations() {
			return rels.size();
		}

		public long size() {
			return nodes.size() + refNodesBuffer.size() + ways.size() + refWaysBuffer.size() + rels.size();
		}

		public boolean isEmpty() {
			return cNodes == 0 && cRefNodes == 0 && cWays == 0 && cRefWays == 0 && cRels == 0 && cRefRels == 0;
		}
		//
		// public void clear() {
		// nodes.clear();
		// ways.clear();
		// rels.clear();
		//
		// cNodes = cWays = cRels = 0;
		// cRefNodes = cRefWays = cRefRels = 0;
		//
		// refNodes.clear();
		// refWays.clear();
		//
		// refNodesBuffer.clear();
		// refWaysBuffer.clear();
		// }

		@Override
		public String toString() {
			return String.format("%2d:%d [%d,%d - %d,%d - %d] %d", ZGrid.getZoom(cellId),
					ZGrid.getIdWithoutZoom(cellId), cNodes, cRefNodes, cWays, cRefWays, cRels, size());
		}
	}

	private final PeekingIterator<CellData> entityReader;

	private final PeekingIterator<CellBitmaps> bitmapReader;
	private final Handler handler;

	private Grid[] entityGrid;

	public LoaderGrid(PeekingIterator<CellData> entityReader, PeekingIterator<CellBitmaps> bitmapReader,
			Handler handler) {
		this.entityReader = entityReader;
		this.bitmapReader = bitmapReader;
		this.handler = handler;

		entityGrid = new Grid[OSHDB.MAXZOOM + 1];
	}
	
	boolean debug = false; 
	long cellIdContainsNode = -1;
	public void run() throws IOException {
		while (entityReader.hasNext()) {
			long cellId = entityReader.peek().cellId;
			final int zoom = ZGrid.getZoom(cellId);
			
			loadLowerZoom(cellId);

			if (zoom == 0 && ZGrid.getIdWithoutZoom(cellId) != 0) {
				System.out.printf("zoom=%d, id:%d, cellId:%d%n", zoom, ZGrid.getIdWithoutZoom(cellId), cellId);
				cellId = 0;
			}
			final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(cellId);

			while (bitmapReader.hasNext()
					&& ZGrid.ORDER_DFS_TOP_DOWN.compare(bitmapReader.peek().cellId, cellId) <= 0) {
				CellBitmaps cellBitmaps = bitmapReader.next();
				final Grid grid = getGrid(cellBitmaps.cellId);// entityGrid[bitmapZoom];
				grid.refNodes.or(cellBitmaps.nodes);
				grid.refWays.or(cellBitmaps.ways);
				while (bitmapReader.hasNext() && ZGrid.ORDER_DFS_TOP_DOWN.compare(bitmapReader.peek().cellId, cellBitmaps.cellId) == 0) {
					// merge bitmaps
					cellBitmaps = bitmapReader.next();
					grid.refNodes.or(cellBitmaps.nodes);
					grid.refWays.or(cellBitmaps.ways);
				}			
			}

			final Buffer nodes = new Buffer();
			final Buffer ways = new Buffer();
			final Buffer rels = new Buffer();

			int cNodes = 0;
			int cWays = 0;
			int cRels = 0;

			final Grid grid = getGrid(cellId); // entityGrid[zoom];

			while (entityReader.hasNext()
					&& ZGrid.ORDER_DFS_BOTTOM_UP.compare(entityReader.peek().cellId, cellId) == 0) {
				final CellData cellData = entityReader.next();
				final byte[] data = cellData.bytes;
				switch (cellData.type) {
				case NODE: {
					final TransformOSHNode osh = TransformOSHNode.instance(data, 0, data.length, 0, 0,0, 0);

					if (handler.filterNode(osh)) {
						nodes.writeInt(data.length);
						nodes.write(data);
						cNodes++;
					} else {
						long parentCellId = cellId;
						while (true) {
							// for (int z = zoom; z >= 0; z--) {
							final Grid parent = getGrid(parentCellId, false);// entityGrid[z];
							if (parent != null && parent.refNodes.contains(osh.getId())) {
								parent.refNodesBuffer.writeInt(data.length);
								parent.refNodesBuffer.write(data);
								parent.cRefNodes++;
								break;
							}

							if (parentCellId == 0) {
								break;
							}
							parentCellId = ZGrid.getParent(parentCellId);
						}
					}
					break;
				}
				case WAY: {
					ways.writeInt(data.length);
					ways.write(data);
					cWays++;
					break;
				}
				case RELATION:
					rels.writeInt(data.length);
					rels.write(data);
					cRels++;
					break;
				default:
					return;
				}
			}

			if (cNodes > 0) {
				if (grid.nodes.size() == 0) {
					grid.nodes = nodes;
				} else {
					grid.nodes.writeAll(nodes);
				}
				grid.cNodes += cNodes;
			}

			if (cWays > 0) {
				if (grid.ways.size() == 0) {
					grid.ways = ways;
				} else {
					grid.ways.writeAll(ways);
				}
				grid.cWays += cWays;
			}

			if (cRels > 0) {
				if (grid.rels.size() == 0) {
					grid.rels = rels;
				} else {
					grid.rels.writeAll(rels);
				}
				grid.cRels += cRels;
			}
			load(cellId);
		}
	}
	
	
	private void moveGridUp(long cellId) throws IOException{
		int zoom = ZGrid.getZoom(cellId);
		Grid grid = entityGrid[zoom];
			
		long parentCellId = ZGrid.getParent(cellId);
		
		Grid parentGrid = entityGrid[zoom -1];
		if(parentGrid != null){
			int c = ZGrid.ORDER_DFS_BOTTOM_UP.compare(parentGrid.cellId, parentCellId);
			if(c < 0){
				moveGridUp(parentGrid.cellId);
			}else if(c > 0){
				System.out.println("moveGridUp wrong parent zoom:"+zoom+" "+parentGrid.cellId+" > "+parentCellId+" grid: "+grid);
				System.exit(2);
			}
		}
		load(grid.cellId);
		entityGrid[zoom] = null;
	}

	private void loadLowerZoom(long cellId) throws IOException {
		int zoom = ZGrid.getZoom(cellId);
		for (int z = OSHDB.MAXZOOM; z >= zoom; z--) {
			if(z == 0){
				return;
			}			
			Grid grid = entityGrid[z];
			
			if (grid != null && grid.cellId != cellId) {
				moveGridUp(grid.cellId);
			}
		}

		// load up to common parent
		long parentCellId = ZGrid.getParent(cellId);
		zoom--;
		while (true) {
			Grid grid = entityGrid[zoom];
			if (grid != null) {
				if (ZGrid.ORDER_DFS_BOTTOM_UP.compare(grid.cellId, parentCellId) < 0) {
					moveGridUp(grid.cellId);
				}else{
					return;
				}
			}
			if(parentCellId == 0){
				return;
			}
			parentCellId = ZGrid.getParent(parentCellId);
			zoom --;
		}
	}

	private Grid getGrid(long cellId) {
		return getGrid(cellId, true);
	}

	private Grid getGrid(long cellId, boolean init) {
		final int zoom = ZGrid.getZoom(cellId);
		Grid grid = entityGrid[zoom];
		if (grid == null) {
			if (!init)
				return null;
			grid = new Grid();
			grid.cellId = cellId;
			entityGrid[zoom] = grid;
		}
		if(!init && grid.cellId != cellId){
			return null;
		}

		if (grid.cellId != cellId) {
			System.err.println("getGrid "+grid.cellId+" != "+cellId);
			System.exit(2);
		}
		return grid;
	}

	private void clearGrid(long cellId) {
		final int zoom = ZGrid.getZoom(cellId);
		entityGrid[zoom] = null;
	}

	private void moveUpRelation(Grid grid, Grid parent) throws IOException {
		try (final BufferedSource buffer = grid.rels.peek()) {
			while (!buffer.exhausted()) {
				int length = buffer.readInt();
				byte[] data = new byte[length];
				buffer.readFully(data);
				TransformOSHRelation osh = TransformOSHRelation.instance(data, 0, length);
				parent.refNodes.add(osh.getNodeIds());
				parent.refWays.add(osh.getWayIds());
			}
		}
		parent.rels.writeAll(grid.rels);
		parent.cRels += grid.cRels;
		grid.cRels = 0;

		if (grid.cRefWays > 0) {
			try (final BufferedSource buffer = grid.refWaysBuffer.peek()) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
					parent.refNodes.add(osh.getNodeIds());
				}
			}
			parent.refWaysBuffer.writeAll(grid.refWaysBuffer);
			parent.cRefWays += grid.cRefWays;
			grid.cRefWays = 0;
		}
	}

	private void moveUpWay(Grid grid, Grid parent) throws IOException {
		try (final BufferedSource buffer = grid.ways.peek()) {
			while (!buffer.exhausted()) {
				int length = buffer.readInt();
				byte[] data = new byte[length];
				buffer.readFully(data);
				TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
				parent.refNodes.add(osh.getNodeIds());
				parent.refNodes.or(grid.refNodes);
			}
		}

		parent.ways.writeAll(grid.ways);
		parent.cWays += grid.cWays;
		if (grid.cRefNodes > 0) {
			parent.refNodesBuffer.writeAll(grid.refNodesBuffer);
			parent.cRefNodes += grid.cRefNodes;
			grid.cRefNodes = 0;
		}
	}

	private void moveUpNode(Grid grid, Grid parent) throws IOException {
		if (grid.cNodes == 0)
			return;

		parent.nodes.writeAll(grid.nodes);
		parent.cNodes += grid.cNodes;
		grid.cNodes = 0;
	}

	private void load(long cellId) throws IOException {
		final Grid grid = getGrid(cellId);// entityGrid[z];

		final boolean loadRelation = handler.loadRelCondition(grid) || cellId == 0;
		final boolean loadWay = handler.loadWayCondition(grid) || cellId == 0;
		final boolean loadNode = handler.loadNodeCondition(grid) || cellId == 0;

		if (!loadRelation) {
			long parentCellId = ZGrid.getParent(cellId);
			final Grid parent = getGrid(parentCellId);// entityGrid[z - 1];
			moveUpRelation(grid, parent);

			if (!loadWay) {
				moveUpWay(grid, parent);

				if (!loadNode) {
					moveUpNode(grid, parent);
					clearGrid(cellId);
					return;
				}
			}
		}

		final List<TransformOSHRelation> oshRelations;
		if (loadRelation) {
			oshRelations = new ArrayList<>(grid.cRels);
			Buffer buffer = grid.rels;
			while (!buffer.exhausted()) {
				int length = buffer.readInt();
				byte[] data = new byte[length];
				buffer.readFully(data);
				TransformOSHRelation osh = TransformOSHRelation.instance(data, 0, length);
				oshRelations.add(osh);
			}
		} else {
			oshRelations = Collections.emptyList();
		}

		final List<TransformOSHWay> oshWays;
		if (loadRelation || loadWay) {
			oshWays = new ArrayList<>(grid.cWays + grid.cRefWays);

			try (final BufferedSource buffer = grid.ways.peek()) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
					oshWays.add(osh);
					if (cellId > 0 && loadWay) {
						long parentCellId = ZGrid.getParent(cellId);
						while (true) {
							final Grid parent = getGrid(parentCellId, false);
							if (parent != null && parent.refWays.contains(osh.getId())) {
								parent.refWaysBuffer.writeInt(length);
								parent.refWaysBuffer.write(data);
								parent.cRefWays++;
								parent.refNodes.add(osh.getNodeIds());
								break;
							}
							if (parentCellId == 0) {
								break;
							}
							parentCellId = ZGrid.getParent(parentCellId);
						}
					}
				}
			}

			if (!loadWay) {
				long parentCellId = ZGrid.getParent(cellId);
				final Grid parent = getGrid(parentCellId);// entityGrid[z - 1];
				parent.ways.writeAll(grid.ways);
				parent.cWays += grid.cWays;
				for (TransformOSHWay osh : oshWays) {
					parent.refNodes.add(osh.getNodeIds());
					parent.refNodes.or(grid.refNodes);
				}
			}
			if (loadRelation) {
				try (final Buffer buffer = grid.refWaysBuffer) {
					while (!buffer.exhausted()) {
						int length = buffer.readInt();
						byte[] data = new byte[length];
						buffer.readFully(data);
						TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
						oshWays.add(osh);
						if (cellId > 0) {
							long parentCellId = ZGrid.getParent(cellId);
							while (true) {
								final Grid parent = getGrid(parentCellId, false);
								if (parent != null && parent.refWays.contains(osh.getId())) {
									parent.refWaysBuffer.writeInt(length);
									parent.refWaysBuffer.write(data);
									parent.cRefWays++;
									parent.refNodes.add(osh.getNodeIds());
									break;
								}
								if (parentCellId == 0) {
									break;
								}
								parentCellId = ZGrid.getParent(parentCellId);
							}
						}
					}
				}
			}
		} else {
			oshWays = Collections.emptyList();
		}

		final List<TransformOSHNode> oshNodes;
		if (loadRelation || loadWay || loadNode) {
			oshNodes = new ArrayList<>(grid.cNodes + grid.cRefNodes);

			try (final BufferedSource buffer = grid.nodes.peek()) {
				while (!buffer.exhausted()) {
						final int length = buffer.readInt();
						final byte[] data = new byte[length];
						try {
							buffer.readFully(data);
						} catch (EOFException eof) {
							System.out.printf("z:%2d, %s, cellId:%d, length:%d%n", ZGrid.getZoom(grid.cellId), grid,
									grid.cellId, length);
							throw eof;
						}
						final TransformOSHNode osh = TransformOSHNode.instance(data, 0, length, 0, 0,0, 0);
						oshNodes.add(osh);
						
						if (cellId > 0 && loadNode) {
							long parentCellId = ZGrid.getParent(cellId);
							while (true) {
								final Grid parent = getGrid(parentCellId, false);
								if (parent != null && parent.refNodes.contains(osh.getId())) {
									parent.refNodesBuffer.writeInt(length);
									parent.refNodesBuffer.write(data);
									parent.cRefNodes++;
									break;
								}
								if (parentCellId == 0) {
									break;
								}
								parentCellId = ZGrid.getParent(parentCellId);
							}
						}
				}
			}

			if (!loadNode && grid.cNodes > 0) {
				final long parentCellId = ZGrid.getParent(cellId);
				final Grid parent = getGrid(parentCellId);
				parent.nodes.writeAll(grid.nodes);
				parent.cNodes += grid.cNodes;
				grid.cNodes = 0;
			}

			if (loadWay || loadRelation) {
				try (final Buffer buffer = grid.refNodesBuffer) {
					while (!buffer.exhausted()) {
						final int length = buffer.readInt();
						final byte[] data = new byte[length];
						buffer.readFully(data);
						final TransformOSHNode osh = TransformOSHNode.instance(data, 0, length, 0, 0, 0, 0);
						oshNodes.add(osh);
												
						if (cellId > 0) {
							long parentCellId = ZGrid.getParent(cellId);
							while (true) {
								final Grid parent = getGrid(parentCellId, false);
								if (parent != null && parent.refNodes.contains(osh.getId())) {
									parent.refNodesBuffer.writeInt(length);
									parent.refNodesBuffer.write(data);
									parent.cRefNodes++;
									break;
								}
								if (parentCellId == 0) {
									break;
								}
								parentCellId = ZGrid.getParent(parentCellId);
							}
						}
					}
				}
			}

		} else {
			oshNodes = Collections.emptyList();
		}

		if (loadRelation) {
			handler.handleRelationGrid(grid.cellId, oshRelations, oshNodes, oshWays);
		}
		if (loadWay) {
			handler.handleWayGrid(grid.cellId, oshWays.subList(0, grid.cWays), oshNodes);
		}
		if (loadNode) {
			handler.handleNodeGrid(grid.cellId, oshNodes.subList(0, grid.cNodes));
		}
		clearGrid(cellId); // grid.clear();
	}
}
