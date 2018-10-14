package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.rocksdb.RocksDbIdToCellSink;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import com.google.common.base.Stopwatch;

public class TransformNode extends Transformer {

	public TransformNode(TagToIdMapper tagToId, CellDataMap cellDataMap, IdToCellSink idToCellSink) {
		super(tagToId,cellDataMap,idToCellSink);
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
	
	@Override
	protected long transform(long id, OSMType type, List<Entity> versions) throws IOException {
		final List<OSMNode> nodes = new ArrayList<>(versions.size());
		final Set<Long> cellIds = new TreeSet<>();
		for (Entity version : versions) {
			final Node node = (Node) version;
			if (version.isVisible()) {
				final long zId = getCell(node.getLongitude(), node.getLatitude());
				if (zId >= 0) {
					cellIds.add(zId);
				} else {
					// System.err.printf("negative zId! %s%n", node);
				}
			}
			nodes.add(getNode(node));
		}
		
		final long cellId = (cellIds.size() > 0) ? findBestFittingCellId(cellIds) : -1;
		final OSHDBBoundingBox bbox = getCellBounce(cellId);
		final long baseLongitude = bbox.getMinLonLong();
		final long baseLatitude = bbox.getMinLatLong();
		final long baseId = 0;
	
		final TransformOSHNode osh = TransformOSHNode.build(baData, baRecord, baAux, nodes, baseId, 0L, baseLongitude, baseLatitude);
		final ByteBuffer record = ByteBuffer.wrap(baRecord.array(),0,baRecord.length());
		
		store(cellId, record);
		idToCell(id, cellId);
				
		return id;
	}
	

	
	
	public static void main(String[] args) throws IOException {
		
		Args config = Transform.parse(args);
		if(config == null)
			return;
		
		//Transform transform = Transform.parse(args);
		//Path dataDir = config.Paths.get("/home/rtroilo/data");

		Path workDir = config.workDir; //dataDir.resolve("work/germany2");
		Path pbf = config.pbf; //dataDir.resolve("germany.osh.pbf");

		int workerId = config.worker; //1;
		int workerTotal = config.totalWorkers; //5;
		
		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);
		
		long start = pbfMeta.nodeStart;
		long end = pbfMeta.nodeEnd;
		long hardEnd = pbfMeta.nodeEnd;
		
		long chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd = chunkStart;
		for (int i = 0; i <= workerId; i++) {
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart + chunkSize, end);
		}		
		
		System.out.printf("start:%d,end:%d%n",chunkStart,chunkEnd);
		Transform transform = Transform.of(pbf,chunkStart,chunkEnd,end,workerId);
	
		final long memDataMap = 30L * 1024L * 1024L * 1024L;
		final TagToIdMapper tagToId = TransformerTagRoles.getTagToIdMapper(workDir);
		try (final CellDataMap cellDataMap = new CellDataMap(workDir, String.format("transform_node_%02d", workerId),memDataMap); 
			 final Options options = new Options();
			EnvOptions envOptions = new EnvOptions()) {
			
			BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
			blockTableConfig.setBlockSize(1024L*1024L*1L); // 1MB
			blockTableConfig.setCacheIndexAndFilterBlocks(true);
			options.setTableFormatConfig(blockTableConfig);
			options.setWriteBufferSize(1024L*1024L*128L);
			
			String sstFileName = String.format("transform_idToCell_node_%02d.sst",workerId);
			String sstFilePath = workDir.resolve(sstFileName).toString();
			
			try(IdToCellSink idToCellSink = RocksDbIdToCellSink.open(sstFilePath, options, envOptions)){
				TransformNode node = new TransformNode(tagToId, cellDataMap, idToCellSink);
				Stopwatch stopwatch = Stopwatch.createStarted();
				transform.transform(node,()-> {System.out.println("complete!");});
				System.out.println(stopwatch);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
