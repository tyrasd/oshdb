import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import com.google.common.io.MoreFiles;


public class Import {
  static final int MAX_ZOOM = 14;
  static final long MIN_BYTES = 4L * 1024 * 1024;
  static final long MAX_BYTES = 64L * 1024 * 1024;

  public static class IdOffsetList {
    public final long zId;
    public final List<OffsetSize> offsets;

    public IdOffsetList(long zId, List<OffsetSize> offsets) {
      this.zId = zId;
      this.offsets = offsets;
    }
  }

  public static class OffsetSize implements Comparable<OffsetSize> {
    public final long offset;
    public final int size;

    public static OffsetSize of(long offset, int size) {
      return new OffsetSize(offset, size);
    }

    private OffsetSize(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }

    @Override
    public int compareTo(OffsetSize o) {
      int c = Long.compare(offset, o.offset);
      return c;
    }
  }

  @FunctionalInterface
  protected static interface GridInstance {
    void apply(long zId, List<byte[]> buffers) throws IOException;
  }
  
  protected static long getXYFromZId(long zId) {
    final int zoom = ZGrid.getZoom(zId);
    final XYGrid xyGrid = new XYGrid(zoom);
    final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
    long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
    long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
    long xyId = xyGrid.getId(baseLongitude, baseLatitude);
    return xyId;
  }
  
  private static SortedMap<Long, List<OffsetSize>> prepare(Path index) throws IOException {
    TreeMap<Long, List<OffsetSize>> resultMap = new TreeMap<>(ZGrid.ORDER_DFS_BOTTOM_UP);
    TreeMap<Long, List<OffsetSize>> cellIdOffsets = new TreeMap<>(ZGrid.ORDER_DFS_BOTTOM_UP);

    try (DataInputStream indexStream =
        new DataInputStream(MoreFiles.asByteSource(index).openBufferedStream())) {
      while (indexStream.available() > 0) {
        long zId = indexStream.readLong();
        long offset = indexStream.readLong();
        int bytes = indexStream.readInt();
        List<OffsetSize> offsets =
            cellIdOffsets.computeIfAbsent(Long.valueOf(zId), it -> new ArrayList<>());
        offsets.add(OffsetSize.of(offset, bytes));
      }
    }

    LinkedList<IdOffsetList> list = new LinkedList<>();
    cellIdOffsets.entrySet()
        .forEach(entry -> list.add(new IdOffsetList(entry.getKey(), entry.getValue())));
    cellIdOffsets.clear();

    while (!list.isEmpty()) {
      IdOffsetList entry = list.remove();
      long zId = entry.zId;

      List<OffsetSize> offsets = entry.offsets;
      long size = offsets.stream().mapToLong(os -> os.size).sum();

      if (size < MIN_BYTES) {
        long pZId = ZGrid.getParent(zId);

        ListIterator<IdOffsetList> itr = list.listIterator();
        while (itr.hasNext()) {
          IdOffsetList nextEntry = itr.next();
          long nextZId = nextEntry.zId;
          if (ZGrid.ORDER_DFS_BOTTOM_UP.compare(nextZId, pZId) < 0) {
            continue;
          }

          if (nextZId == pZId) {
            nextEntry.offsets.addAll(offsets);
          } else if (itr.hasPrevious()) {
            itr.previous();
            itr.add(new IdOffsetList(pZId, offsets));
          } else {
            list.addFirst(new IdOffsetList(pZId, offsets));
          }
          break;
        }
      } else if (size >= MAX_BYTES) {
        long pZId = ZGrid.getParent(zId);
        List<OffsetSize> pOffsets;
        int pSize = 0;

        ListIterator<IdOffsetList> itr = list.listIterator();
        while (itr.hasNext()) {
          IdOffsetList nextEntry = itr.next();
          long nextZId = nextEntry.zId;
          if (ZGrid.ORDER_DFS_BOTTOM_UP.compare(nextZId, pZId) < 0) {
            continue;
          }

          if (nextZId == pZId) {
            pOffsets = nextEntry.offsets;
            pSize = pOffsets.stream().mapToInt(os -> os.size).sum();
          } else {
            pOffsets = new ArrayList<>();
          }
          offsets.sort((a, b) -> Integer.compare(a.size, b.size));

          OffsetSize biggest = offsets.get(offsets.size() - 1);
          while ((pSize + biggest.size) < MAX_BYTES && (size) >= MAX_BYTES) {
            pOffsets.add(biggest);
            offsets.remove(offsets.size() - 1);
            pSize += biggest.size;
            size -= biggest.size;
            if (offsets.isEmpty())
              break;
            biggest = offsets.get(offsets.size() - 1);
          }

          if (size >= MAX_BYTES) {
            OffsetSize smallest = offsets.get(0);
            while ((pSize + smallest.size) < MAX_BYTES && (size) >= MAX_BYTES) {
              pOffsets.add(smallest);
              offsets.remove(0);
              pSize += smallest.size;
              size -= smallest.size;

              if (offsets.isEmpty())
                break;
              smallest = offsets.get(0);
            }
          }

          if (nextZId != pZId) {
            if (itr.hasPrevious()) {
              itr.previous();
              itr.add(new IdOffsetList(pZId, pOffsets));
            } else {
              list.addFirst(new IdOffsetList(pZId, pOffsets));
            }
          }
          break;
        }
        resultMap.put(Long.valueOf(zId), offsets);
      } else {
        resultMap.put(Long.valueOf(zId), offsets);
      }
    }
    return resultMap;
  }

  public static void stream(String type, Path workDir,GridInstance grid) throws IOException {
      Path typeIndex = workDir.resolve(type + ".index");
      Path typeData = workDir.resolve(type + ".data");

      SortedMap<Long, List<OffsetSize>> resultMap = prepare(typeIndex);
      List<byte[]> buffers = new ArrayList<>();
      try (RandomAccessFile raf = new RandomAccessFile(typeData.toFile(), "r")) {
        Iterator<Entry<Long, List<OffsetSize>>> itr = resultMap.entrySet().stream().iterator();
        while (itr.hasNext()) {
          Entry<Long, List<OffsetSize>> e = itr.next();
          long zId = e.getKey().longValue();

          List<OffsetSize> offsets = e.getValue();
          buffers.clear();
          for (OffsetSize os : offsets) {
            long offset = os.offset;
            raf.seek(offset);
            long osZid = raf.readLong();
            int sizeEntities = raf.readInt();
            for (int i = 0; i < sizeEntities; i++) {
              int length = raf.readInt();
              byte[] buffer = new byte[length];
              raf.readFully(buffer);
              buffers.add(buffer);
            }
          }
          grid.apply(zId, buffers);
        }
      }
  }
}
