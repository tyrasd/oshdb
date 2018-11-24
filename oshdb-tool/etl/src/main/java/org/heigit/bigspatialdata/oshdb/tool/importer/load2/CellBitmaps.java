package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.nio.file.Path;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class CellBitmaps {
	public final Path readerId;
	public final long cellId;
	public final Roaring64NavigableMap nodes;
	public final Roaring64NavigableMap ways;

	public CellBitmaps(Path readerId, long cellId, Roaring64NavigableMap nodes, Roaring64NavigableMap ways) {
		this.readerId = readerId;
		this.cellId = cellId;
		this.nodes = nodes;
		this.ways = ways;
	}
	
	@Override
	public String toString() {
		return String.format("n:%d,w:%d", nodes.getLongCardinality(),ways.getLongCardinality());
	}
}
