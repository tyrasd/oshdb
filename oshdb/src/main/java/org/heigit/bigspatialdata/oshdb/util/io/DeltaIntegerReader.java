package org.heigit.bigspatialdata.oshdb.util.io;

import java.io.IOException;
import java.io.InputStream;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.IntegerReader;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class DeltaIntegerReader implements LongIterator {

  private static class InternalInStream extends InStream {
    private InputStream input;
    private int length;
    private int read = 0;
    
    public InternalInStream(String name) {
      super(name, -1);
    }
    
    public void setInputStream(InputStream input, int length){
      this.input = input;
      this.length = length;
      this.read = 0;
    }
    
    @Override
    public void close() {
      // TODO Auto-generated method stub
    }
    
    @Override
    public int available() throws IOException {
      return Math.min(input.available(),length-read);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read() throws IOException {
      return input.read();
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return input.read(b, off, len);
    }
  }
  
  private final IntegerReader reader;
  private InternalInStream input; 
  
  public DeltaIntegerReader(String name, boolean signed) {
    input = new InternalInStream(name);
    boolean skipCorrupt = false;
    try {
      reader = new RunLengthIntegerReaderV2(input, signed, skipCorrupt);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  public void setInputStream(InputStream input, int length) {
    this.input.setInputStream(input,length);
  }

  public long read() throws IOException{
    return reader.next();
  }
  
  public boolean hasNext() {
    try {
      return reader.hasNext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public long nextLong() {
    try {
      return reader.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
