package org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader;

import java.io.IOException;
import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.LoaderGrid.Grid;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler.OSHDBHandler;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

public abstract class GridLoader extends OSHDBHandler {
	public static final long MB = 1L*1024L*1024L;

	@Override
	public void handleNodeGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		final int zoom = ZGrid.getZoom(zId);
		final XYGrid xyGrid = new XYGrid(zoom);
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(baseLongitude, baseLatitude);
		
		handleNodeGrid(new GridOSHNodes(xyId, zoom, 0L, 0L, baseLongitude, baseLatitude, Arrays.copyOf(offsets, size), data),seq);
	}

	public abstract void handleNodeGrid(GridOSHNodes gridOSHNodes, int seq);

	@Override
	public void handleWayGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		final int zoom = ZGrid.getZoom(zId);
		final XYGrid xyGrid = new XYGrid(zoom);
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(baseLongitude, baseLatitude);
		
		handleWayGrid(new GridOSHWays(xyId, zoom, 0L, 0L, baseLongitude, baseLatitude, Arrays.copyOf(offsets, size), data),seq);
	}

	public abstract void handleWayGrid(GridOSHWays gridOSHWays,int seq);

	@Override
	public void handleRelationGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException {
		final int zoom = ZGrid.getZoom(zId);
		final XYGrid xyGrid = new XYGrid(zoom);
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(baseLongitude, baseLatitude);
		
		handleRelationGrid(new GridOSHRelations(xyId, zoom, 0L, 0L, baseLongitude, baseLatitude, Arrays.copyOf(offsets, size), data),seq);
	}

	public abstract void handleRelationGrid(GridOSHRelations gridOSHRelations,int seq);	
	
	@Override
	public boolean loadNodeCondition(Grid grid) {
		if ((grid.countNodes() > 1000 && grid.sizeNodes() >= 2L * MB) || (grid.countWays() >= 10 &&  grid.sizeNodes() >= 4L * MB)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean loadWayCondition(Grid grid) {
		long size = grid.sizeNodes() + grid.sizeRefNodesWay() + grid.sizeWays();
		if ((grid.countWays() > 1000 && size >= 2L * MB) || (grid.countWays() >= 10 && size >= 4L * MB)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean loadRelCondition(Grid grid) {
		long size = grid.sizeRefRelWays() + grid.sizeRefNodesRel() + grid.sizeRelations();
		if ((grid.countRelations() > 1000 && size >= 2L * MB) || (grid.countRelations() >= 10 && size >= 4L * MB)) {
			return true;
		}
		return false;
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
		return  true;
	}
	
	@Override
	public boolean filterRelation(TransformOSHRelation osh) {
		return true;
	}

}
