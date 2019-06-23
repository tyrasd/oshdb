import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.util.bytearray.ByteArrayOutputWrapper;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.io.CountingOutputStream;

public class ImportUpdateDB extends Import {
  
  public static class Args {
    @Parameter(names = {"-work", "--workingDir"},
        description = "path to store the result files.", validateWith = DirExistValidator.class,
        required = true, order = 10)
    public Path workDir;

    @Parameter(names = {"-out"}, description = "output path", required = true)
    public Path output;
  }

  public static void main(String[] args) throws IOException{  
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
    final Path outDir = config.output;
    
    Files.createDirectories(outDir);
    
    ByteArrayOutputWrapper encode = new ByteArrayOutputWrapper(1024);
    write("relation", workDir, outDir, encode, (data) -> OSHRelation.instance(data, 0, data.length).getId());
    write("way", workDir, outDir, encode, (data) -> OSHWay.instance(data, 0, data.length).getId());
    write("node", workDir, outDir, encode, (data) -> OSHNode.instance(data, 0, data.length).getId());
  }

  @FunctionalInterface
  private interface ToLongFunctionIO<T> {
    long applyAsLong(T value) throws IOException;
  }
  
  private static void write(String type, Path workDir, Path outDir, ByteArrayOutputWrapper encode,
      ToLongFunctionIO<byte[]> getId) throws IOException, FileNotFoundException {
    try(RandomAccessFile index = new RandomAccessFile(outDir.resolve(type+".idx").toString(), "rw");
       CountingOutputStream data = new CountingOutputStream(new FileOutputStream(outDir.resolve(type+".data").toString()))){    
      stream(type, workDir, (zId, buffers) -> {
        final int zoom = ZGrid.getZoom(zId);
        long xyId = getXYFromZId(zId);
        
        for(byte[] buffer : buffers) {
          long id = getId.applyAsLong(buffer);
          long offset = data.getCount();
          encode.reset();
          
          encode.writeUInt32(zoom);
          encode.writeUInt64(xyId);
          encode.writeUInt32(buffer.length);
          encode.writeByteArray(buffer);
          
          index.seek(id*8);
          index.writeLong(offset);
          data.write(encode.array());
        }
      });
    }
  }

}
