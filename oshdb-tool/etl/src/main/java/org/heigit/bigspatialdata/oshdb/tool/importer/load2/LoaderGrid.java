package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler.Handler;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.google.common.collect.PeekingIterator;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import okio.Buffer;
import okio.BufferedSource;

public class LoaderGrid {

	public static class Grid {
		public long cellId;
		public int zoom;

		Buffer nodes = new Buffer();
		Buffer ways = new Buffer();
		Buffer rels = new Buffer();

		int cNodes, cRefNodesWay, cRefNodesRel;
		int cWays, cRefWaysRel;
		int cRels, cRefRels;

		Buffer refNodesWayBuffer = new Buffer();
		Buffer refNodesRelBuffer = new Buffer();
		Buffer refWaysRelBuffer = new Buffer();

		public int countNodes() {
			return cNodes;
		}

		public long sizeNodes() {
			return nodes.size();
		}

		public long sizeRefNodesWay() {
			return refNodesWayBuffer.size();
		}

		public long sizeRefNodesRel() {
			return refNodesRelBuffer.size();
		}

		public int countWays() {
			return cWays;
		}

		public long sizeWays() {
			return ways.size();
		}

		public long sizeRefRelWays() {
			return refWaysRelBuffer.size();
		}

		public int countRelations() {
			return cRels;
		}

		public long sizeRelations() {
			return rels.size();
		}

		public long size() {
			return nodes.size() + refNodesWayBuffer.size() + refNodesRelBuffer.size() + ways.size()
					+ refWaysRelBuffer.size() + rels.size();
		}

		public boolean isEmpty() {
			return cNodes == 0 && cRefNodesWay == 0 && cRefNodesRel == 0 && cWays == 0 && cRefWaysRel == 0 && cRels == 0
					&& cRefRels == 0;
		}

		@Override
		public String toString() {
			return String.format("%2d:%d [n(%d,%d,%d) - w(%d,%d) - r(%d)] %d", ZGrid.getZoom(cellId),
					ZGrid.getIdWithoutZoom(cellId), cNodes, cRefNodesWay, cRefNodesRel, cWays, cRefWaysRel, cRels,
					size());
		}
	}

	private final PeekingIterator<CellData> entityReader;

	// private final PeekingIterator<CellBitmaps> bitmapReader;
	private final Handler handler;
	private final Long2ObjectAVLTreeMap<CellBitmaps> bitmapsWay = new Long2ObjectAVLTreeMap<>();
	private final Long2ObjectAVLTreeMap<CellBitmaps> bitmapsRel = new Long2ObjectAVLTreeMap<>();
	private Grid[] entityGrid;

	public LoaderGrid(PeekingIterator<CellData> entityReader, PeekingIterator<CellBitmaps> bitmapReaderWay, PeekingIterator<CellBitmaps> bitmapReaderRel, Handler handler) {
		this.entityReader = entityReader;
		// this.bitmapReader = bitmapReader;
		this.handler = handler;

		while (bitmapReaderWay.hasNext()) {
			final CellBitmaps cellBitmaps = bitmapReaderWay.next();
			final long cellId = cellBitmaps.cellId;
			while (bitmapReaderWay.hasNext()
					&& ZGrid.ORDER_DFS_TOP_DOWN.compare(bitmapReaderWay.peek().cellId, cellId) == 0) {
				// merge bitmaps
				CellBitmaps merge = bitmapReaderWay.next();
				cellBitmaps.nodes.or(merge.nodes);
				cellBitmaps.ways.or(merge.ways);
			}

			cellBitmaps.nodes.runOptimize();
			cellBitmaps.ways.runOptimize();
			bitmapsWay.put(cellId, cellBitmaps);
		}

		while (bitmapReaderRel.hasNext()) {
			final CellBitmaps cellBitmaps = bitmapReaderRel.next();
			final long cellId = cellBitmaps.cellId;
			while (bitmapReaderRel.hasNext()
					&& ZGrid.ORDER_DFS_TOP_DOWN.compare(bitmapReaderRel.peek().cellId, cellId) == 0) {
				// merge bitmaps
				CellBitmaps merge = bitmapReaderRel.next();
				cellBitmaps.nodes.or(merge.nodes);
				cellBitmaps.ways.or(merge.ways);
			}

			cellBitmaps.nodes.runOptimize();
			cellBitmaps.ways.runOptimize();
			bitmapsRel.put(cellId, cellBitmaps);
		}

		entityGrid = new Grid[OSHDB.MAXZOOM + 1];
	}

	boolean debug = false;
	long cellIdContainsNode = -1;

	

	public void run() throws IOException {
		while (entityReader.hasNext()) {
			long cellId = entityReader.peek().cellId;
			final int zoom = ZGrid.getZoom(cellId);
			if(zoom == 0){
				cellId = 0;
			}

			loadLowerZoom(cellId);

			if (zoom == 0 && ZGrid.getIdWithoutZoom(cellId) != 0) {
				System.out.printf("zoom=%d, id:%d, cellId:%d%n", zoom, ZGrid.getIdWithoutZoom(cellId), cellId);
				cellId = 0;
			}

			final Buffer nodes = new Buffer();
			final Buffer ways = new Buffer();
			final Buffer rels = new Buffer();

			int cNodes = 0;
			int cWays = 0;
			int cRels = 0;

			final Grid grid = getGrid(cellId);

			while (entityReader.hasNext()){
				long peekCellId = entityReader.peek().cellId;
				if(zoom == 0){
					int peekZoom = ZGrid.getZoom(peekCellId);
					if(peekZoom == 0){
						peekCellId = 0;
					}
				}
				if(ZGrid.ORDER_DFS_BOTTOM_UP.compare(entityReader.peek().cellId, cellId) != 0){
					break;
				}
				
				final CellData cellData = entityReader.next();
				final byte[] data = cellData.bytes;

				switch (cellData.type) {

				case RELATION:
					rels.writeInt(data.length);
					rels.write(data);
					cRels++;
					if(cellData.id == Debug.rel){
						System.out.println("read rel:"+Debug.rel+" ,cellId:"+cellId);
					}
					break;

				case WAY: {
					ways.writeInt(data.length);
					ways.write(data);
					cWays++;
					if (cellData.id == Debug.way) {
						System.out.println("read way:" + Debug.way + ", cellId:" + cellId);
					}
					break;
				}

				case NODE: {
					final TransformOSHNode osh = TransformOSHNode.instance(data, 0, data.length, 0, 0, 0, 0);
					if (osh.getId() == Debug.node) {
						System.out.println("read node " + Debug.node + ", cellId:" + cellId);
					}
					if (handler.filterNode(osh)) {
						nodes.writeInt(data.length);
						nodes.write(data);
						cNodes++;
					} else {
						nodeRef(cellId, osh.getId(), data);
					}
					break;
				}
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

	private void moveGridUp(long cellId) throws IOException {
		int zoom = ZGrid.getZoom(cellId);
		Grid grid = entityGrid[zoom];

		long parentCellId = ZGrid.getParent(cellId);

		Grid parentGrid = entityGrid[zoom - 1];
		if (parentGrid != null) {
			int c = ZGrid.ORDER_DFS_BOTTOM_UP.compare(parentGrid.cellId, parentCellId);
			if (c < 0) {
				moveGridUp(parentGrid.cellId);
			} else if (c > 0) {
				System.out.println("moveGridUp wrong parent zoom:" + zoom + " " + parentGrid.cellId + " > "
						+ parentCellId + " grid: " + grid);
				System.exit(2);
			}
		}
		load(grid.cellId);
		entityGrid[zoom] = null;
	}

	private void loadLowerZoom(long cellId) throws IOException {
		int zoom = ZGrid.getZoom(cellId);
		for (int z = OSHDB.MAXZOOM; z >= zoom; z--) {
			if (z == 0) {
				return;
			}
			final Grid grid = entityGrid[z];
			if (grid != null && grid.cellId != cellId) {
				moveGridUp(grid.cellId);
			}
		}
		if (zoom == 0)
			return;

		// load up to common parent
		long parentCellId = ZGrid.getParent(cellId);
		zoom--;
		while (true) {
			final Grid grid = entityGrid[zoom];
			if (grid != null) {
				if (ZGrid.ORDER_DFS_BOTTOM_UP.compare(grid.cellId, parentCellId) < 0) {
					moveGridUp(grid.cellId);
				} else {
					return;
				}
			}
			if (parentCellId == 0) {
				return;
			}
			parentCellId = ZGrid.getParent(parentCellId);
			if(zoom == 0)
				return;
			zoom--;
		}
	}

	private Grid getGrid(long cellId) {
		final int zoom = ZGrid.getZoom(cellId);
		Grid grid = entityGrid[zoom];
		if (grid == null) {
			grid = new Grid();
			grid.zoom = zoom;
			grid.cellId = (zoom > 0)?cellId:0;
			entityGrid[zoom] = grid;
		}

		return grid;
	}

	private void clearGrid(long cellId) {
		final int zoom = ZGrid.getZoom(cellId);
		entityGrid[zoom] = null;
		bitmapsWay.remove(cellId);
		bitmapsRel.remove(cellId);
	}

	private void moveUpRelation(Grid grid, Grid parent) throws IOException {
		if (grid.cRels == 0)
			return;
		CellBitmaps currentBitmaps = bitmapsRel.get(grid.cellId);
		CellBitmaps parentBitmaps = bitmapsRel.get(parent.cellId);
		
		if (parentBitmaps == null) {
			bitmapsRel.put(parent.cellId,
					new CellBitmaps(null, parent.cellId, currentBitmaps.nodes, currentBitmaps.ways));
		} else {
			parentBitmaps.nodes.or(currentBitmaps.nodes);
			parentBitmaps.ways.or(currentBitmaps.ways);
		}

		if (parent.cRels == 0) {
			parent.rels = grid.rels;
		} else {
			parent.rels.writeAll(grid.rels);
		}

		parent.cRels += grid.cRels;
		grid.cRels = 0;

		if (grid.cRefNodesRel > 0) {
			if (parent.cRefNodesRel == 0) {
				parent.refNodesRelBuffer = grid.refNodesRelBuffer;
			} else {
				parent.refNodesRelBuffer.writeAll(grid.refNodesRelBuffer);
			}
			parent.cRefNodesRel += grid.cRefNodesRel;
		}

		if (grid.cRefWaysRel > 0) {
			if (parent.cRefWaysRel == 0) {
				parent.refWaysRelBuffer = grid.refWaysRelBuffer;
			} else {
				parent.refWaysRelBuffer.writeAll(grid.refWaysRelBuffer);
			}
			parent.cRefWaysRel += grid.cRefWaysRel;
		}
	}

	private void moveUpWay(Grid grid, Grid parent) throws IOException {
		if (grid.cWays == 0)
			return;
		CellBitmaps currentBitmaps = bitmapsWay.get(grid.cellId);
		CellBitmaps parentBitmaps = bitmapsWay.get(parent.cellId);
		if (parentBitmaps == null) {
			bitmapsWay.put(parent.cellId, new CellBitmaps(null, parent.cellId, currentBitmaps.nodes, null));
		} else {
			parentBitmaps.nodes.or(currentBitmaps.nodes);
		}

		if (parent.cWays == 0) {
			parent.ways = grid.ways;
		} else {
			parent.ways.writeAll(grid.ways);
		}
		parent.cWays += grid.cWays;
		grid.cWays = 0;

		if (grid.cRefNodesWay > 0) {
			if (parent.cRefNodesWay == 0) {
				parent.refNodesWayBuffer = grid.refNodesWayBuffer;
			} else {
				parent.refNodesWayBuffer.writeAll(grid.refNodesWayBuffer);
			}
			parent.cRefNodesWay += grid.cRefNodesWay;
		}
	}

	private void moveUpNode(Grid grid, Grid parent) throws IOException {
		if (grid.cNodes == 0)
			return;
		if (parent.cNodes == 0) {
			parent.nodes = grid.nodes;
		} else {
			parent.nodes.writeAll(grid.nodes);
		}
		parent.cNodes += grid.cNodes;
		grid.cNodes = 0;
	}

	private void load(long cellId) throws IOException {
		final Grid grid = getGrid(cellId);

		final boolean loadRelation = handler.loadRelCondition(grid) || cellId == 0;
		final boolean loadWay = handler.loadWayCondition(grid) || cellId == 0;
		final boolean loadNode = handler.loadNodeCondition(grid) || cellId == 0;

		List<TransformOSHRelation> oshRelations = Collections.emptyList();
		List<TransformOSHWay> oshWays = Collections.emptyList();
		List<TransformOSHNode> oshNodes = Collections.emptyList();

		final Long2ObjectRBTreeMap<TransformOSHWay> wayIdTransformWay = new Long2ObjectRBTreeMap<>();
		final Long2ObjectRBTreeMap<TransformOSHNode> nodeIdTransformNode = new Long2ObjectRBTreeMap<>();

		long parentCellId = ZGrid.getParent(cellId);
		final Grid parent = getGrid(parentCellId);

		if (loadRelation) {
			oshRelations = new ArrayList<>(grid.cRels);
			try (Buffer buffer = grid.rels) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHRelation osh = TransformOSHRelation.instance(data, 0, length);
					oshRelations.add(osh);
					if(osh.getId() == Debug.rel){
						System.out.println("load rel:"+Debug.rel+" at "+cellId);
					}
				}
			}
			try (Buffer buffer = grid.refWaysRelBuffer) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
					wayIdTransformWay.put(osh.getId(), osh);
					if(osh.getId() == Debug.way){
						System.out.println("add refRelWay "+osh.getId()+" to wayIdTransformNode");
					}
					if(cellId > 0){
						wayRef(ZGrid.getParent(cellId), osh.getId(), data, osh.getNodeIds());
					}
				}
			}
			try (Buffer buffer = grid.refNodesRelBuffer) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
					nodeIdTransformNode.put(osh.getId(), osh);
					if(Debug.node == osh.getId()){
						System.out.println("add refRelNode "+Debug.node+" to nodeIdTransformNode");
					}
					if(cellId > 0){
						nodeRef(ZGrid.getParent(cellId), osh.getId(), data);
					}
				}
			}

			if (!loadWay) {
				try (final BufferedSource buffer = grid.ways.peek()) {
					while (!buffer.exhausted()) {
						int length = buffer.readInt();
						byte[] data = new byte[length];
						buffer.readFully(data);
						TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
						wayIdTransformWay.putIfAbsent(osh.getId(), osh);
					
						if(osh.getId() == Debug.way){
							System.out.println("add Way "+osh.getId()+" to wayIdTransformNode");
						}
					}
				}
				try (final BufferedSource buffer = grid.refNodesWayBuffer.peek()) {
					while (!buffer.exhausted()) {
						int length = buffer.readInt();
						byte[] data = new byte[length];
						buffer.readFully(data);
						TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
						nodeIdTransformNode.putIfAbsent(osh.getId(), osh);
						if(Debug.node == osh.getId()){
							System.out.println("add refWayNode:"+Debug.node+" to nodeIdTransformNode.list");
						}
					}
				}
			}

			if (!loadNode) {
				try (final BufferedSource buffer = grid.nodes.peek()) {
					while (!buffer.exhausted()) {
						final int length = buffer.readInt();
						final byte[] data = new byte[length];
						buffer.readFully(data);
						final TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
						if(Debug.node == osh.getId()){
							System.out.println("add node:"+Debug.node+" to nodeIdTransformNode.list");
						}
						nodeIdTransformNode.putIfAbsent(osh.getId(), osh);
					}
				}
			}
		}else{
			moveUpRelation(grid, parent);
		}

		if (loadWay) {
			oshWays = new ArrayList<>(grid.cWays);
			try (final Buffer buffer = grid.ways) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHWay osh = TransformOSHWay.instance(data, 0, length);
					oshWays.add(osh);
					if(osh.getId() == Debug.way){
						System.out.println("load way:"+osh.getId());
					}
					if(cellId > 0){
						wayRef(ZGrid.getParent(cellId), osh.getId(), data, osh.getNodeIds());
					}
				}
			}
			try (final Buffer buffer = grid.refNodesWayBuffer) {
				while (!buffer.exhausted()) {
					int length = buffer.readInt();
					byte[] data = new byte[length];
					buffer.readFully(data);
					TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
					nodeIdTransformNode.putIfAbsent(osh.getId(), osh);
					if (osh.getId() == Debug.node) {
						System.out.println("add refWayNode:" + Debug.node);
					}
					if(cellId > 0){
						nodeRef(ZGrid.getParent(cellId), osh.getId(), data);
					}
				}
			}

			if (!loadNode) {
				try (final BufferedSource buffer = grid.nodes.peek()) {
					while (!buffer.exhausted()) {
						final int length = buffer.readInt();
						final byte[] data = new byte[length];
						buffer.readFully(data);
						final TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
						nodeIdTransformNode.putIfAbsent(osh.getId(), osh);
					}
				}
			}
		}else{
			moveUpWay(grid, parent);
		}

		if (loadNode) {
			oshNodes = new ArrayList<>(grid.cNodes);
			try (final Buffer buffer = grid.nodes) {
				while (!buffer.exhausted()) {
					final int length = buffer.readInt();
					final byte[] data = new byte[length];
					buffer.readFully(data);
					final TransformOSHNode osh = TransformOSHNode.instance(data, 0, length);
					oshNodes.add(osh);
					if (Debug.node == osh.getId()) {
						System.out.println("load node " + Debug.node + ", cellId:" + cellId);
					}
					if(cellId > 0){
						nodeRef(ZGrid.getParent(cellId), osh.getId(), data);
					}
				}
			}
		}else{
			moveUpNode(grid, parent);
		}
		

		

		if (loadRelation) {
			for (TransformOSHNode node : oshNodes) {
				nodeIdTransformNode.putIfAbsent(node.getId(), node);
			}
			for (TransformOSHWay way : oshWays) {
				wayIdTransformWay.putIfAbsent(way.getId(), way);
			}
			handler.handleRelationGrid(grid.cellId, oshRelations, nodeIdTransformNode, wayIdTransformWay);
		}
		if (loadWay) {
			for (TransformOSHNode node : oshNodes) {
				nodeIdTransformNode.putIfAbsent(node.getId(), node);
			}
			handler.handleWayGrid(grid.cellId, oshWays, nodeIdTransformNode);
		}
		if (loadNode) {
			handler.handleNodeGrid(grid.cellId, oshNodes);
		}
		clearGrid(cellId); // grid.clear();
	}

	private void wayRef(long cellId, long id, byte[] data, long[] nodeIds) {
		long parentCellId = cellId;
		while (true) {
			CellBitmaps cbR = bitmapsRel.get(parentCellId);
			if (cbR != null && cbR.ways.contains(id)) {
				final Grid parent = getGrid(parentCellId);
				parent.refWaysRelBuffer.writeInt(data.length);
				parent.refWaysRelBuffer.write(data);
				parent.cRefWaysRel++;
				cbR.nodes.add(nodeIds);
				break;
			}

			if (parentCellId == 0) {
				break;
			}
			parentCellId = ZGrid.getParent(parentCellId);
		}
	}

	private void nodeRef(long cellId, long id, byte[] data) {
		long parentCellId = cellId;

		while (true) {
			if (bitmapsWay.isEmpty())
				break;

			CellBitmaps cb = bitmapsWay.get(parentCellId);

			if (cb != null && cb.nodes.contains(id)) {
				final Grid parent = getGrid(parentCellId);
				parent.refNodesWayBuffer.writeInt(data.length);
				parent.refNodesWayBuffer.write(data);
				parent.cRefNodesWay++;

				if (Debug.node == id) {
					System.out.println("add node " + id + " to wayRef " + parent.cellId);
				}
				break;
			}

			if (parentCellId == 0) {
				break;
			}
			parentCellId = ZGrid.getParent(parentCellId);
		}
		parentCellId = cellId;
		while (true) {
			if (bitmapsRel.isEmpty())
				break;
			CellBitmaps cb = bitmapsRel.get(parentCellId);
			if (cb != null && cb.nodes.contains(id)) {
				final Grid parent = getGrid(parentCellId);
				parent.refNodesRelBuffer.writeInt(data.length);
				parent.refNodesRelBuffer.write(data);
				parent.cRefNodesRel++;
				if (Debug.node == id) {
					System.out.println("add node " + id + " to relRef " + parent.cellId);
				}
				break;
			}
			if (parentCellId == 0) {
				break;
			}
			parentCellId = ZGrid.getParent(parentCellId);
		}
	}
}
