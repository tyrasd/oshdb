package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping.CellDataSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobReader;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class TransformNode extends Transformer {

	public TransformNode(TagToIdMapper tagToId, CellDataSink cellDataMap, IdToCellSink idToCellSink) {
		super(tagToId, cellDataMap, idToCellSink);
	}

	private OSMNode getNode(Node entity) {
		return new OSMNode(entity.getId(), //
				modifiedVersion(entity), //
				new OSHDBTimestamp(entity.getTimestamp()), //
				entity.getChangeset(), //
				entity.getUserId(), //
				getKeyValue(entity.getTags()), //
				entity.getLongitude(), entity.getLatitude());
	}

	private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);

	@Override
	protected OSMType getType() {
		return OSMType.NODE;
	}

	final List<Long> testIds = Lists.newArrayList(103561L, 158334L, 158335L, 158336L);
	@Override
	protected long transform(long id, OSMType type, List<Entity> versions) throws IOException {
		final List<OSMNode> nodes = new ArrayList<>(versions.size());
		final Set<Long> cellIds = new TreeSet<>();
		for (Entity version : versions) {
			final Node node = (Node) version;
			if (version.isVisible()) {
				final long zId = getCell(node.getLongitude(), node.getLatitude());
//				if(zId <= 0){
//					System.out.printf("%10d(%2d), %10d %10d -> z:%2d,%10d(%10d)%n",id,version.getVersion(), node.getLongitude(),node.getLatitude(),ZGrid.getIdWithoutZoom(zId),ZGrid.getIdWithoutZoom(zId),zId);
//				}
				if (zId >= 0) {
					cellIds.add(zId);
				} else {
					// System.err.printf("negative zId! %s%n", node);
				}
			}
			nodes.add(getNode(node));
		}

		final long cellId = (cellIds.size() > 0) ? findBestFittingCellId(cellIds) : -1;
		
		if(testIds.stream().anyMatch(it -> it.longValue() == id)){
			System.out.printf("%10d [%s] -> %d%n",id,Iterables.toString(cellIds),cellId);
		}
		
		final OSHDBBoundingBox bbox = getCellBounce(cellId);
		final long baseLongitude = bbox.getMinLonLong();
		final long baseLatitude = bbox.getMinLatLong();
		final long baseId = 0;

		final TransformOSHNode osh = TransformOSHNode.build(baData, baRecord, baAux, nodes, baseId, 0L, baseLongitude,
				baseLatitude);
		final ByteBuffer record = ByteBuffer.wrap(baRecord.array(), 0, baRecord.length());

		store(cellId, record);
		idToCell(id, cellId);

		return id;
	}

	public static void transform(Args args,TagToIdMapper tagToId, CellDataSink cellDataSink, IdToCellSink idToCellSink) throws IOException {
		Path pbf = args.pbf;

		int workerId = args.worker;
		int workerTotal = args.totalWorkers;

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);
		final long start = pbfMeta.nodeStart;
		final long end = pbfMeta.nodeEnd;
		final long hardEnd = pbfMeta.nodeEnd;

		long chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd = chunkStart;
		for (int i = 0; i <= workerId; i++) {
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart + chunkSize, end);
		}

		final Transform transform = Transform.of(pbf, chunkStart, chunkEnd, end, workerId);
		TransformNode node = new TransformNode(tagToId, cellDataSink, idToCellSink);
		Stopwatch stopwatch = Stopwatch.createStarted();
		transform.transform(node, () -> {
			System.out.println("complete!");
		});
		System.out.println(stopwatch);

	}
}
