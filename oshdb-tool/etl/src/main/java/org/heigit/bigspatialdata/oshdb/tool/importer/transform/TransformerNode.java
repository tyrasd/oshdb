package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellRefMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;


public class TransformerNode extends Transformer {

  private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);

  public TransformerNode(long maxMemory,int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper,CellDataMap cellDataMap, CellRefMap cellRefMap, int workerId) throws IOException {
    super(maxMemory,maxZoom, workDirectory, tagToIdMapper,cellDataMap, cellRefMap, workerId);
  }

  public OSMType type() {
    return OSMType.NODE;
  }

  long lastId = -1;
  public void transform(long id, List<Entity> versions) {
		  
	if(id < lastId){
		System.out.println("skipping node! id not in order! id:"+id+" previous id:"+lastId);
		return;
	}
	lastId = id;
	  
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

    try {

   ///   final OSHDBBoundingBox bbox = getCellBounce(cellId);

      final long baseLongitude = 0; // bbox.getMinLonLong();
      final long baseLatitude = 0; //bbox.getMinLatLong();

      final LongFunction<ByteBuffer> toByteArray = baseId -> {
        try {
                    
          final TransformOSHNode osh = TransformOSHNode.build(baData, baRecord, baAux, nodes, baseId, 0L, baseLongitude, baseLatitude);
                    
//          final byte[] record = new byte[baRecord.length()];
//          System.arraycopy(baRecord.array(), 0, record, 0, record.length);
                    
          return ByteBuffer.wrap(baRecord.array(),0,baRecord.length());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

      store(cellId,id,toByteArray);
      addIdToCell(id, cellId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
}
