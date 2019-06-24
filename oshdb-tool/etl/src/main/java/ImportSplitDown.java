import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.ProgressUtil;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.io.MoreFiles;

public class ImportSplitDown {
  static final int MAX_ZOOM = 14;
  static final long MIN_BYTES = 4L * 1024 * 1024;
  static final long MAX_BYTES = 64L * 1024 * 1024;

  public static class Args {
    @Parameter(names = {"-workDir", "--workingDir"},
        description = "path to store the result files.", validateWith = DirExistValidator.class,
        required = true, order = 10)
    public Path workDir;

    @Parameter(names = {"-ignite", "-igniteConfig", "-icfg"},
        description = "Path ot ignite-config.xml", required = true, order = 1)
    public Path ignitexml;

    @Parameter(names = {"--prefix"}, description = "cache table prefix", required = false)
    public String prefix;
  }

  public static void main(String[] args) throws IOException {
    Args config = new Args();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();

    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      System.out.println(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();
      return;
    }

    final Path workDir = config.workDir;
    final Path igniteConfig = config.ignitexml;
    final String prefix = config.prefix;

    final boolean disableWal = true;

    Ignition.setClientMode(true);
    try (Ignite ignite = Ignition.start(igniteConfig.toString())) {
      ignite.cluster().active(true);
      try {

        stream("node", prefix, workDir, ignite, disableWal,
            (xyId, zoom, index, data) -> new GridOSHNodes(xyId, zoom, 0L, 0L, 0L, 0L, index, data));
        stream("way", prefix, workDir, ignite, disableWal,
            (xyId, zoom, index, data) -> new GridOSHWays(xyId, zoom, 0L, 0L, 0L, 0L, index, data));
        stream("relation", prefix, workDir, ignite, disableWal, (xyId, zoom, index,
            data) -> new GridOSHRelations(xyId, zoom, 0L, 0L, 0L, 0L, index, data));

      } finally {
        if (disableWal) {
          System.out.println("deactive cluster");
          ignite.cluster().active(false);
        }
        ignite.cluster().active(true);
      }
    }
  }

  @FunctionalInterface
  private static interface GridInstance<T> {
    T get(long xyId, int zoom, int[] index, byte[] data);
  }

  private static <T> void stream(String type, String prefix, Path workDir, Ignite ignite,
      boolean disableWal, GridInstance<T> gridInstance) throws IOException {
    
    final String cacheName = prefix + "_grid_" + type;
    System.out.println("# import "+type+" to "+cacheName);
    System.out.println("#");
    System.out.println("zoom,xyid,count,bytes,hrbc,time");
    try (IgniteDataStreamer<Long, T> stream = openStreamer(ignite, cacheName, disableWal)) {
      Path typeIndex = workDir.resolve(type + ".index");
      Path typeData = workDir.resolve(type + ".data");
      load(typeIndex, typeData, (zId, buffers) -> {
        try {
          final int zoom = ZGrid.getZoom(zId);
          long xyId = getXYFromZId(zId);

          int[] index = new int[buffers.size()];
          ByteArrayOutputStream aux = new ByteArrayOutputStream();

          int offset = 0;
          int i = 0;
          for (byte[] buffer : buffers) {
            index[i++] = offset;
            aux.write(buffer);
            offset += buffer.length;
          }
          byte[] bytes = aux.toByteArray();

          System.out.printf("%2d,%12d,%5d,%10d,\"%s\"", zoom, xyId, buffers.size(), bytes.length, ProgressUtil.hRBC(bytes.length));
          
          Stopwatch stopwatch = Stopwatch.createStarted();
          T grid = gridInstance.get(xyId, zoom, index, bytes);
          final long levelId = CellId.getLevelId(zoom, xyId);
          stream.addData(levelId, grid);
          stream.flush();
          System.out.printf(",\"%s\"%n", stopwatch);
        } catch (IOException io) {
          io.printStackTrace();
        }
      });
    } finally {
      if (disableWal) {
        ignite.cluster().enableWal(cacheName);
      }
    }
  }

  private static long getXYFromZId(long zId) {
    final int zoom = ZGrid.getZoom(zId);
    final XYGrid xyGrid = new XYGrid(zoom);
    final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
    long baseLongitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
    long baseLatitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
    long xyId = xyGrid.getId(baseLongitude, baseLatitude);
    return xyId;
  }

  private static <T> IgniteDataStreamer<Long, T> openStreamer(Ignite ignite, String cacheName,
      boolean disableWal) {
    ignite.destroyCache(cacheName);
    CacheConfiguration<Long, T> cacheCfg = new CacheConfiguration<>(cacheName);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    cacheCfg.setCopyOnRead(false);
    RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();
    affFunc.setExcludeNeighbors(true);
    affFunc.setPartitions(10_000);
    cacheCfg.setAffinity(affFunc);

    IgniteCache<Long, T> cache = ignite.getOrCreateCache(cacheCfg);

    if (disableWal) {
      ignite.cluster().disableWal(cacheName);
    }

    return ignite.dataStreamer(cache.getName());
  }

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
  private interface BuildGrid {
    void build(long zId, List<byte[]> buffers);
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
      //int zoom = ZGrid.getZoom(zId);

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

    System.out.println("number of partitions:" + resultMap.size());

    // return Collections.emptySortedMap();
    return resultMap;
  }

  public static void load(Path index, Path data, BuildGrid grid) throws IOException {

    SortedMap<Long, List<OffsetSize>> resultMap = prepare(index);

    List<byte[]> buffers = new ArrayList<>();
    try (RandomAccessFile raf = new RandomAccessFile(data.toFile(), "r")) {

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
        grid.build(zId, buffers);
      }
    }
  }

 
}

