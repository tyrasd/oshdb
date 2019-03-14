package org.heigit.bigspatialdata.oshdb.impl.partition.v0_5;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;

public class GridOSHNodes extends GridOSHEntities {

  public GridOSHNodes(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  public Iterable<OSHNode> getEntities() throws IOException{
    return iterable(bb -> OSHNodeImpl.instance(bb.array(), bb.position(), bb.remaining(), baseId, baseTimestamp, baseLongitude, baseLatitude));
  }

}
