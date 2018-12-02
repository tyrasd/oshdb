package org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public abstract class OSHDBHandler implements Handler {

	private static final long MAX_GRID_SIZE = 1L * 1024L * 1024L * 1024L;

	public abstract void handleNodeGrid(long zId, int seq, int[] offsets, int size, byte[] data) throws IOException;

	public abstract void handleWayGrid(long zId, int seq, int[] offsets, int size, byte[] data) throws IOException;

	public abstract void handleRelationGrid(long zId, int seq, int[] offsets, int size, byte[] data) throws IOException;

	private ByteBuffer buildOSHNodeRecord(TransformOSHNode node, long baseId, long baseTimestamp, long baseLongitude,
			long baseLatitude) throws IOException {
		return OSHNode.buildRecord(node.getId(), node, baseId, baseTimestamp, baseLongitude, baseLatitude);
	}

	private OSHNode buildOSHNode(TransformOSHNode node, long baseId, long baseTimestamp, long baseLongitude,
			long baseLatitude) throws IOException {
		ByteBuffer record = buildOSHNodeRecord(node, baseId, baseTimestamp, baseLongitude, baseLatitude);
		return OSHNode.instance(record.array(), 0, record.limit(), baseId, baseTimestamp, baseLongitude, baseLatitude);
	}

	private final ObjectArrayList<OSHNode> wayNodes = new ObjectArrayList<>();

	private ByteBuffer buildOSHWayRecord(TransformOSHWay way, List<OSHNode> nodes, long baseId, long baseTimestamp,
			long baseLongitude, long baseLatitude) throws IOException {
		ByteBuffer record = OSHWay.buildRecord(way.getId(), way, nodes, baseId, baseTimestamp, baseLongitude,
				baseLatitude);
		return record;
	}

	private OSHWay buildOSHWay(TransformOSHWay way, List<OSHNode> nodes, long baseId, long baseTimestamp,
			long baseLongitude, long baseLatitude) throws IOException {
		ByteBuffer record = buildOSHWayRecord(way, nodes, baseId, baseTimestamp, baseLongitude, baseLatitude);
		return OSHWay.instance(record.array(), 0, record.limit(), baseId, baseTimestamp, baseLongitude, baseLatitude);
	}

	public abstract void missingNode(long id);

	public abstract void missingWay(long id);

	private final ObjectArrayList<OSHNode> relationNodes = new ObjectArrayList<>();
	private final ObjectArrayList<OSHWay> relationWays = new ObjectArrayList<>();

	private ByteBuffer buildOSHRelationRecord(TransformOSHRelation relation, Map<Long, OSHNode> nodes,
			Map<Long, OSHWay> ways, long baseId, long baseTimestamp, long baseLongitude, long baseLatitude)
			throws IOException {
		relationNodes.ensureCapacity(relation.getNodeIds().length);
		for (long id : relation.getNodeIds()) {
			OSHNode node = nodes.get(id);
			if (node == null) {
				missingNode(id);
				continue;
			}
			relationNodes.add(node);
		}
		relationWays.ensureCapacity(relation.getWayIds().length);
		for (long id : relation.getWayIds()) {
			OSHWay way = ways.get(id);
			if (way == null) {
				missingWay(id);
				continue;
			}
			relationWays.add(way);
		}

		ByteBuffer record = OSHRelation.buildRecord(relation.getId(), relation, relationNodes, relationWays, baseId,
				baseTimestamp, baseLongitude, baseLatitude);
		relationNodes.clear();
		relationWays.clear();
		return record;
	}

	private OSHRelation buildOSHRelation(TransformOSHRelation relation, Map<Long, OSHNode> nodes,
			Map<Long, OSHWay> ways, long baseId, long baseTimestamp, long baseLongitude, long baseLatitude)
			throws IOException {
		ByteBuffer record = buildOSHRelationRecord(relation, nodes, ways, baseId, baseTimestamp, baseLongitude,
				baseLatitude);
		return OSHRelation.instance(record.array(), 0, record.limit(), baseId, baseTimestamp, baseLongitude,
				baseLatitude);
	}

	@Override
	public void handleNodeGrid(long zId, List<TransformOSHNode> nodes) throws IOException {
		if (zId < 0 || nodes.size() == 0)
			return;
		// final int zoom = ZGrid.getZoom(zId);
		// final XYGrid xyGrid = new XYGrid(zoom);

		final long baseId = 0;
		final long baseTimestamp = 0;
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		final long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		final long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		// final long xyId = xyGrid.getId(baseLongitude, baseLatitude);

		Collections.sort(nodes);

		final ByteArrayOutputStream out = new ByteArrayOutputStream();

		final int[] offsets = new int[nodes.size()];

		final Iterator<TransformOSHNode> itr = nodes.iterator();

		int offset = 0;
		int size = 0;
		int seq = 0;
		while (itr.hasNext()) {
			TransformOSHNode node = itr.next();
			ByteBuffer record = OSHNode.buildRecord(node.getId(), node, baseId, baseTimestamp, baseLongitude,
					baseLatitude);
			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if (offset >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleNodeGrid(zId, seq++, offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}
	}

	@Override
	public void handleWayGrid(long zId, List<TransformOSHWay> ways, List<TransformOSHNode> nodes) throws IOException {
		if (zId < 0 || ways.size() == 0)
			return;
		// final int zoom = ZGrid.getZoom(zId);
		// final XYGrid xyGrid = new XYGrid(zoom);

		final long baseId = 0;
		final long baseTimestamp = 0;
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		final long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		final long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		// final long xyId = xyGrid.getId(baseLongitude, baseLatitude);

		final Map<Long, TransformOSHNode> nodeIdTransformNode = new HashMap<>(nodes.size());

		final Map<Long, OSHNode> nodeIdOshMap = new HashMap<>(nodes.size());
		for (TransformOSHNode node : nodes) {
			nodeIdTransformNode.put(node.getId(), node);
		}

		Collections.sort(ways);

		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final int[] offsets = new int[ways.size()];

		final Iterator<TransformOSHWay> itr = ways.iterator();

		int offset = 0;
		int size = 0;
		int seq = 0;
		while (itr.hasNext()) {
			final TransformOSHWay way = itr.next();
			wayNodes.ensureCapacity(way.getNodeIds().length);
			for (long nodeId : way.getNodeIds()) {
				OSHNode osh = nodeIdOshMap.get(nodeId);
				if (osh == null) {
					TransformOSHNode node = nodeIdTransformNode.get(nodeId);
					if (node != null) {
						osh = buildOSHNode(node, 0, 0, 0, 0);
						nodeIdOshMap.put(osh.getId(), osh);
					}
				}
				if (osh != null) {
					wayNodes.add(osh);
				} else {
					missingNode(nodeId);
				}
			}
			final ByteBuffer record = buildOSHWayRecord(way, wayNodes, baseId, baseTimestamp, baseLongitude,
					baseLatitude);
			wayNodes.clear();

			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if (offset >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleWayGrid(zId, seq++, offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}

	}

	@Override
	public void handleRelationGrid(long zId, List<TransformOSHRelation> relations, List<TransformOSHNode> nodes,
			List<TransformOSHWay> ways) throws IOException {
		if (zId < 0 || relations.size() == 0)
			return;

		// final int zoom = ZGrid.getZoom(zId);
		// final XYGrid xyGrid = new XYGrid(zoom);

		final long baseId = 0;
		final long baseTimestamp = 0;
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		final long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		final long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		// final long xyId = xyGrid.getId(baseLongitude, baseLatitude);

		final Map<Long, TransformOSHNode> nodeIdTransformNode = new HashMap<>(nodes.size());
		final Map<Long, OSHNode> nodeIdOshMap = new HashMap<>(nodes.size());
		for (TransformOSHNode node : nodes) {
			nodeIdTransformNode.put(node.getId(), node);
		}

		final Map<Long, TransformOSHWay> tWayIdOshMap = new HashMap<>(ways.size());
		for (TransformOSHWay way : ways) {
			tWayIdOshMap.put(way.getId(), way);
		}

		Collections.sort(relations);

		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final int[] offsets = new int[relations.size()];

		final Iterator<TransformOSHRelation> itr = relations.iterator();

		int offset = 0;
		int size = 0;
		int seq = 0;
		while (itr.hasNext()) {
			final TransformOSHRelation relation = itr.next();
			final Map<Long, OSHWay> wayIdOshMap = new HashMap<>(relation.getWayIds().length);
			for (long wid : relation.getWayIds()) {
				final TransformOSHWay way = tWayIdOshMap.get(wid);
				if (way != null) {
					wayNodes.ensureCapacity(way.getNodeIds().length);
					for (long nodeId : way.getNodeIds()) {
						OSHNode osh = nodeIdOshMap.get(nodeId);
						if (osh == null) {
							TransformOSHNode node = nodeIdTransformNode.get(nodeId);
							if (node != null) {
								osh = buildOSHNode(node, 0, 0, 0, 0);
								nodeIdOshMap.put(osh.getId(), osh);
							}
						}
						if (osh != null) {
							wayNodes.add(osh);
						} else {
							missingNode(nodeId);
						}
					}

					final OSHWay osh = buildOSHWay(way, wayNodes, 0, 0, 0, 0);
					wayNodes.clear();
					wayIdOshMap.put(osh.getId(), osh);
				}else{
					missingWay(wid);
				}
			}

			ByteBuffer record = buildOSHRelationRecord(relation, nodeIdOshMap, wayIdOshMap, baseId, baseTimestamp,
					baseLongitude, baseLatitude);
			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if (offset >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleRelationGrid(zId, seq++, offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}
	}

}
