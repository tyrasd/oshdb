package org.heigit.bigspatialdata.oshdb.impl.partition.v0_5;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.partition.Partition;
import org.heigit.bigspatialdata.oshdb.partition.PartitionInfo;
import org.heigit.bigspatialdata.oshdb.util.bytearray.ByteArrayWrapper;
import org.heigit.bigspatialdata.oshdb.util.function.IOFunction;

public abstract class GridOSHEntities extends GridOSHInfo implements Partition {
  protected final int size;

  protected final byte[] data;
  protected final int offset;
  protected final int length;

  protected long baseId = 0;
  protected long baseTimestamp = 0;
  protected long baseLongitude = 0;
  protected long baseLatitude = 0;

  public GridOSHEntities(ByteBuffer buffer) {
    this.size = buffer.getInt();

    this.data = buffer.array();
    this.offset = buffer.position();
    this.length = buffer.remaining();
  }

  @Override
  public PartitionInfo getInfo() {
    return this;
  }

  public void setBaseId(long baseId) {
    this.baseId = baseId;
  }

  public void setBaseTimestamp(long baseTimestamp) {
    this.baseTimestamp = baseTimestamp;
  }

  public void setBaseLongitude(long baseLongitude) {
    this.baseLongitude = baseLongitude;
  }

  public void setBaseLatitude(long baseLatitude) {
    this.baseLatitude = baseLatitude;
  }

  @Override
  public abstract Iterable<? extends OSHEntity> getEntities() throws IOException;

  protected <T extends OSHEntity> Iterable<T> iterable(IOFunction<ByteBuffer, T> instance)
      throws IOException {
    return () -> {
      return new Iterator<T>() {
        private final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
        private int i = 0;

        private T next = null;
        private Exception e = null;

        @Override
        public boolean hasNext() {
          return (next != null) || ((next = getNext()) != null);
        }

        @Override
        public T next() {
          if (!hasNext()) {
            throw new NoSuchElementException((e != null) ? e.getMessage() : "");
          }
          return null;
        }

        private T getNext() {
          if (i >= size)
            return null;
          try {
            i++;
            int length = wrapper.readUInt32();
            return instance.apply(wrapper.getSharedBuffer(length));
          } catch (IOException e) {
            this.e = e;
          }
          return null;
        }
      };
    };
  }
}
