package org.heigit.bigspatialdata.oshdb.tool.importer.load2.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.Debug;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public abstract class OSHDBHandler implements Handler {

	private static final long MAX_GRID_SIZE = 32L * 1024L * 1024L;

	public abstract void handleNodeGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException;

	public abstract void handleWayGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException;

	public abstract void handleRelationGrid(long zId, int seq, boolean more, int[] offsets, int size, byte[] data) throws IOException;

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
				if(Debug.node == id){
					System.out.println("missing node:"+id+" for rel:"+relation.getId());
				}
				missingNode(id);
				continue;
			}
			relationNodes.add(node);
		}
		relationWays.ensureCapacity(relation.getWayIds().length);
		for (long id : relation.getWayIds()) {
			OSHWay way = ways.get(id);
			if (way == null) {
				if(Debug.way == id){
					System.out.println("missing way:"+id+" for rel:"+relation.getId());
				}
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
			ByteBuffer record = OSHNode.buildRecord(node.getId(), node, baseId, baseTimestamp, baseLongitude,baseLatitude);
			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if ((offset + size*4) >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleNodeGrid(zId, seq++, itr.hasNext(), offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}
	}

	@Override
	public void handleWayGrid(long zId, List<TransformOSHWay> ways, Long2ObjectRBTreeMap<TransformOSHNode> nodeIdTransformNode) throws IOException {
		if (zId < 0 || ways.size() == 0)
			return;

		final long baseId = 0;
		final long baseTimestamp = 0;
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		final long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		final long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;

		final Map<Long, OSHNode> nodeIdOshMap = new HashMap<>(nodeIdTransformNode.size());
		
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
						nodeIdOshMap.put(nodeId, osh);
					}
				}
				if (osh != null) {
					wayNodes.add(osh);
				} else {
					if(Debug.node == nodeId){
						System.out.println("missing node:"+nodeId +" for way:"+way.getId());
					}
					missingNode(nodeId);
				}
			}
			final ByteBuffer record = buildOSHWayRecord(way, wayNodes, baseId, baseTimestamp, baseLongitude,baseLatitude);
			wayNodes.clear();

			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if ((offset + size*4) >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleWayGrid(zId, seq++, itr.hasNext(), offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}
		
		return;
	}

	@Override
	public void handleRelationGrid(long zId, List<TransformOSHRelation> relations, Long2ObjectRBTreeMap<TransformOSHNode> nodeIdTransformNode, Long2ObjectRBTreeMap<TransformOSHWay> wayIdTransformWay) throws IOException {
		if (zId < 0 || relations.size() == 0)
			return;

		final long baseId = 0;
		final long baseTimestamp = 0;
		final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		final long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		final long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;

		final Map<Long, OSHNode> nodeIdOshMap = new HashMap<>(nodeIdTransformNode.size());
		
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
				final TransformOSHWay way = wayIdTransformWay.get(wid);
				if (way != null) {
					wayNodes.ensureCapacity(way.getNodeIds().length);
					for (long nodeId : way.getNodeIds()) {
						OSHNode osh = nodeIdOshMap.get(nodeId);
						if (osh == null) {
							TransformOSHNode node = nodeIdTransformNode.get(nodeId);
							if (node != null) {
								osh = buildOSHNode(node, 0, 0, 0, 0);
								nodeIdOshMap.put(nodeId, osh);
							}
						}
						if (osh != null) {
							wayNodes.add(osh);
						} else {
							if(Debug.node == nodeId){
								System.out.println("missing node:"+nodeId+" for way:"+wid+" for rel:"+relation.getId());
							}
							missingNode(nodeId);
						}
					}

					final OSHWay osh = buildOSHWay(way, wayNodes, 0, 0, 0, 0);
					wayNodes.clear();
					wayIdOshMap.put(osh.getId(), osh);
				}else{
					if(Debug.way == wid){
						System.out.println("missing way:"+wid+" for rel:"+relation.getId());
					}
					missingWay(wid);
				}
			}
			for(long nid: relation.getNodeIds()){
				if(!nodeIdOshMap.containsKey(Long.valueOf(nid))){
					TransformOSHNode node = nodeIdTransformNode.get(nid);
					if(node != null){
						nodeIdOshMap.put(nid, buildOSHNode(node, 0, 0, 0, 0));
					}else{
						if(Debug.node == nid){
							System.out.println("missing node:"+nid+" for rel:"+relation.getId());
						}
						missingNode(nid);
					}
				}
			}

			ByteBuffer record = buildOSHRelationRecord(relation, nodeIdOshMap, wayIdOshMap, baseId, baseTimestamp,
					baseLongitude, baseLatitude);
			offsets[size++] = offset;
			out.write(record.array(), 0, record.limit());
			offset += record.limit();

			if ((offset + size*4) >= MAX_GRID_SIZE || !itr.hasNext()) {
				final byte[] data = out.toByteArray();
				handleRelationGrid(zId, seq++, itr.hasNext(), offsets, size, data);
				offset = size = 0;
				out.reset();
			}
		}
	}

}
