package org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler;

import java.io.IOException;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid.Grid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

public interface Handler {
	boolean loadRelCondition(Grid grid) throws IOException;

	boolean loadWayCondition(Grid grid) throws IOException;

	boolean loadNodeCondition(Grid grid)throws IOException ;

	boolean filterNode(TransformOSHNode osh);
	
	boolean filterWay(TransformOSHWay osh);
	boolean filterRelation(TransformOSHRelation osh);

	
	void handleNodeGrid(long cellId, List<TransformOSHNode> nodes) throws IOException;
	
	void handleRelationGrid(long cellId, List<TransformOSHRelation> oshRelations,
			Long2ObjectRBTreeMap<TransformOSHNode> nodeIdTransformNode,
			Long2ObjectRBTreeMap<TransformOSHWay> wayIdTransformWay) throws IOException;

	void handleWayGrid(long cellId, List<TransformOSHWay> oshWays,
			Long2ObjectRBTreeMap<TransformOSHNode> nodeIdTransformNode) throws IOException;
}
