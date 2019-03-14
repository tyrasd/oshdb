package org.heigit.bigspatialdata.oshdb.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.heigit.bigspatialdata.oshdb.impl.partition.v0_5.GridOSHEntities;
import org.heigit.bigspatialdata.oshdb.impl.partition.v0_5.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.impl.partition.v0_5.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.impl.partition.v0_5.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;


/**
 * 
 * Every OSHDBPartition starts with a sequence of 
 * - 12 bytes 'magic_bytes' representing the string 'oshdb@HeiGIT' 
 * -  4 bytes version major (big endian)
 * -  4 bytes version minor (big endian)
 * -  4 bytes version patch (big endian)
 *
 */
public class PartitionReader {
  public static final byte[] MAGIC_BYTES = "oshdb@HeiGIT".getBytes();

  public Partition read(byte[] bytes) throws IOException {
    return read(bytes, 0, bytes.length);
  }
  
  public Partition read(byte[] bytes, int offset, int length) throws IOException {
    if (!checkMagicBytes(bytes, offset, length)) {
      throw new IOException("bytes does not start with MAGIC_BYTES");
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
    buffer.position(buffer.position() + MAGIC_BYTES.length);

    int major = buffer.getInt();
    int minor = buffer.getInt();
    int patch = buffer.getInt();

    if (major == 0 && minor == 5) {
      return version0_5(major,minor,patch, buffer);
    }
    throw new IOException(
        String.format("unkown partition version (%d.%d.%d)", major, minor, patch));
  }


  private Partition version0_5(int major, int minor, int patch, ByteBuffer buffer) throws IOException {
    int type = buffer.get();
    
    long id = buffer.getLong();
    int level = buffer.getInt();

    long baseId = buffer.getLong();
    long baseTimestamp = buffer.getLong();
    long minLongitude = buffer.getLong();
    long maxLongitude = buffer.getLong();
    long baseLongitude = 0;
    
    long minLatitude = buffer.getLong();
    long maxLatitude = buffer.getLong();
    long baseLatitude = 0;

    final GridOSHEntities grid;
    if( type == 0) {
       grid = new GridOSHNodes(buffer);
    }else  if( type == 1) {
      grid = new GridOSHWays(buffer);
    }else if( type == 2) {
      grid =  new GridOSHRelations(buffer);
    } else {
      throw new IOException("unknown grid type! got type = "+type);
    }
    
    grid.setId(id);
    grid.setLevel(level);
    grid.setBaseId(baseId);
    grid.setBBox(new OSHDBBoundingBox(minLongitude,minLatitude,maxLongitude,maxLatitude));
    grid.setBaseTimestamp(baseTimestamp);
    grid.setBaseLongitude(baseLongitude);
    grid.setBaseLatitude(baseLatitude);
    return grid;
  }

  private boolean checkMagicBytes(byte[] bytes, int offset, int length) {
    for (int i = 0; i < MAGIC_BYTES.length; i++) {
      if (bytes[offset + i] != MAGIC_BYTES[i])
        return false;
    }
    return true;
  }
}
