package org.heigit.bigspatialdata.oshdb.impl.partition.v0_5;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;

public class GridOSHRelations extends GridOSHEntities{

  public GridOSHRelations(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  public Iterable<? extends OSHEntity> getEntities() throws IOException {
    return iterable(bb -> OSHRelationImpl.instance(bb.array(), bb.position(), bb.remaining(), baseId, baseTimestamp, baseLongitude, baseLatitude));
  }

}
