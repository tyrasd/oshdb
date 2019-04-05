import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract.KeyValuePointer;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.io.MoreFiles;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class ImportH2 {
  static final int MAX_ZOOM = 14;
  static final long MIN_BYTES = 4L * 1024 * 1024;
  static final long MAX_BYTES = 64L * 1024 * 1024;

  public static class Args {
    @Parameter(names = {"-workDir", "--workingDir"},
        description = "path to store the result files.", validateWith = DirExistValidator.class,
        required = true, order = 10)
    public Path workDir;

    @Parameter(names = {"-extractDir"},
        description = "extract directory", validateWith = DirExistValidator.class,
        required = true, order = 10)
    public Path extractDir;
    
    @Parameter(names = {"--out"}, description = "output path", required = true)
    public Path output;

    @Parameter(names = {"--oshdb", },
        description = "Path to oshdb", required = true, order = 1)
    public Path oshdb;
  }

  public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
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
    final Path extractDir = config.extractDir;
    final Path outDir = config.output;
    
    final Path oshdb = config.oshdb;

    Class.forName("org.h2.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:h2:" + oshdb.toString() + "", "sa", "");
        Statement stmt = conn.createStatement()) {
        
        stream("node", workDir, conn, outDir,
            (xyId, zoom, index, data) -> new GridOSHNodes(xyId, zoom, 0L, 0L, 0L, 0L, index, data));
        stream("way", workDir, conn, outDir,
            (xyId, zoom, index, data) -> new GridOSHWays(xyId, zoom, 0L, 0L, 0L, 0L, index, data));
        stream("relation", workDir,conn , outDir, (xyId, zoom, index,
            data) -> new GridOSHRelations(xyId, zoom, 0L, 0L, 0L, 0L, index, data));
        
        stmt.executeUpdate(
            "drop table if exists metadata ; create table if not exists metadata (key varchar primary key, value varchar)");
        loadMeta(conn, extractDir.resolve("extract_meta"));
        stmt.executeUpdate(
            "drop table if exists key ; create table if not exists key (id int primary key, txt varchar)");
        stmt.executeUpdate(
            "drop table if exists keyvalue; create table if not exists keyvalue (keyId int, valueId int, txt varchar, primary key (keyId,valueId))");
        loadTags(conn, extractDir.resolve("extract_keys"), extractDir.resolve("extract_keyvalues"));
        stmt.executeUpdate(
            "drop table if exists role ; create table if not exists role (id int primary key, txt varchar)");
        loadRoles(conn, extractDir.resolve("extract_roles"));

    }
  }

  @FunctionalInterface
  private static interface GridInstance<T> {
    T get(long xyId, int zoom, int[] index, byte[] data);
  }

  private static <T> void stream(String type, Path workDir, Connection conn,
      Path outDir, GridInstance<T> gridInstance) throws IOException {

    System.out.println("# import " + type);
    System.out.println("#");
    System.out.println("zoom,xyid,count,bytes,hrbc,time");

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(String.format( //
          "drop table if exists grid_%1$s; " + //
              "create table if not exists grid_%1$s (level int, id bigint, data blob, primary key(level,id))",
          type));
    } catch (SQLException e) {
      throw new IOException(e);
    }

    try (
        DataOutputStream out = new DataOutputStream(
            MoreFiles.asByteSink(outDir.resolve(type + ".data")).openBufferedStream());
        PreparedStatement stmt = conn.prepareStatement(
            String.format("insert into grid_%s (level,id,data) values(?,?,?)", type))) {

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

          System.out.printf("%2d,%12d,%5d,%10d,\"%s\"", zoom, xyId, buffers.size(), bytes.length,
              hRBC(bytes.length));
          Stopwatch stopwatch = Stopwatch.createStarted();
          out.writeLong(zId);
          out.writeInt(bytes.length + index.length * 4 + 4);
          out.writeInt(index.length);
          for (int o : index) {
            out.writeInt(o);
          }
          out.write(bytes);

          T grid = gridInstance.get(xyId, zoom, index, bytes);

          aux.reset();
          try (ObjectOutputStream oos = new ObjectOutputStream(aux)) {
            oos.writeObject(grid);
            oos.flush();
          }
          stmt.setInt(1, zoom);
          stmt.setLong(2, xyId);
          bytes = aux.toByteArray();
          stmt.setBinaryStream(3, new FastByteArrayInputStream(bytes, 0, bytes.length));
          stmt.executeUpdate();

          System.out.printf(",\"%s\"%n", stopwatch);
        } catch (IOException | SQLException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (SQLException e) {
      throw new IOException(e);
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
      int zoom = ZGrid.getZoom(zId);

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

    System.out.println("# number of partitions:" + resultMap.size());
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
  
  public static void loadMeta(Connection conn, Path meta) {
    try (
        PreparedStatement pstmt =
            conn.prepareStatement("insert into metadata (key,value) values(?,?)");
        BufferedReader br = new BufferedReader(new FileReader(meta.toFile()));) {


      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.trim().isEmpty())
          continue;

        String[] split = line.split("=", 2);
        if (split.length != 2)
          throw new RuntimeException("metadata file is corrupt");

        pstmt.setString(1, split[0]);
        pstmt.setString(2, split[1]);
        pstmt.addBatch();
      }


      pstmt.setString(1, "attribution.short");
      pstmt.setString(2, "© OpenStreetMap contributors");
      pstmt.addBatch();

      pstmt.setString(1, "attribution.url");
      pstmt.setString(2, "https://ohsome.org/copyrights");
      pstmt.addBatch();

      pstmt.setString(1, "oshdb.maxzoom");
      pstmt.setString(2, "" + 14);
      pstmt.addBatch();

      pstmt.setString(1, "oshdb.flags");
      pstmt.setString(2, "expand");
      pstmt.addBatch();

      pstmt.executeBatch();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void loadTags(Connection conn, Path keys, Path keyValues) {
    try (
        final DataInputStream keyIn =
            new DataInputStream(MoreFiles.asByteSource(keys).openBufferedStream());
        final RandomAccessFile raf = new RandomAccessFile(keyValues.toFile(), "r");
        final FileChannel valuesChannel = raf.getChannel();) {

      final int length = keyIn.readInt();
      int batch = 0;
      try {

        PreparedStatement insertKey =
            conn.prepareStatement("insert into key (id,txt) values (?,?)");
        PreparedStatement insertValue =
            conn.prepareStatement("insert into keyvalue ( keyId, valueId, txt ) values(?,?,?)");

        for (int keyId = 0; keyId < length; keyId++) {
          final KeyValuePointer kvp = KeyValuePointer.read(keyIn);
          final String key = kvp.key;

          insertKey.setInt(1, keyId);
          insertKey.setString(2, key);
          insertKey.executeUpdate();
          valuesChannel.position(kvp.valuesOffset);

          DataInputStream valueStream = new DataInputStream(Channels.newInputStream(valuesChannel));

          long chunkSize = (long) Math.ceil((double) (kvp.valuesNumber / 10.0));
          int valueId = 0;
          for (int i = 0; i < 10; i++) {
            long chunkEnd = valueId + Math.min(kvp.valuesNumber - valueId, chunkSize);
            for (; valueId < chunkEnd; valueId++) {
              final VF vf = VF.read(valueStream);
              final String value = vf.value;

              insertValue.setInt(1, keyId);
              insertValue.setInt(2, valueId);
              insertValue.setString(3, value);
              insertValue.addBatch();
              batch++;

              if (batch >= 100_000) {
                insertValue.executeBatch();
                batch = 0;
              }
            }
          }
        }
        insertValue.executeBatch();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void loadRoles(Connection conn, Path roles) {
    try (DataInputStream roleIn =
        new DataInputStream(MoreFiles.asByteSource(roles).openBufferedStream())) {
      try {

        PreparedStatement insertRole =
            conn.prepareStatement("insert into role (id,txt) values(?,?)");
        for (int roleId = 0; roleIn.available() > 0; roleId++) {
          final Role role = Role.read(roleIn);

          insertRole.setInt(1, roleId);
          insertRole.setString(2, role.role);
          insertRole.executeUpdate();
          System.out.printf("load role:%6d(%s)%n", roleId, role.role);

        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }
  
  

  public static String hRBC(long bytes) {
    final int unit = 1024;
    if (bytes < unit)
      return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    final String pre = "" + "kMGTPE".charAt(exp - 1);
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }
}

