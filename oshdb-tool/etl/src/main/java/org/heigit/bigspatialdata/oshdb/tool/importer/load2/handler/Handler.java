package org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid.Grid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;

public interface Handler {
	void handleRelationGrid(long cellId, List<TransformOSHRelation> relations, List<TransformOSHNode> nodes, List<TransformOSHWay> ways) throws IOException;

	void handleWayGrid(long cellId, List<TransformOSHWay> ways, List<TransformOSHNode> nodes) throws IOException;

	void handleNodeGrid(long cellId, List<TransformOSHNode> nodes) throws IOException;

	boolean loadRelCondition(Grid grid) throws IOException;

	boolean loadWayCondition(Grid grid) throws IOException;

	boolean loadNodeCondition(Grid grid)throws IOException ;

	boolean filterNode(TransformOSHNode osh);
	
	boolean filterWay(TransformOSHWay osh);
	boolean filterRelation(TransformOSHRelation osh);
}
