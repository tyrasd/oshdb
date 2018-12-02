package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.IOException;
import java.io.InputStream;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.IntegerReader;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class DeltaIntegerReader implements LongIterator {

	@FunctionalInterface
	public static interface ReadConsumer {
		public void read(IntegerReader reader) throws IOException;
	}
	
	private static class InternalInStream extends InStream {
		private InputStream input;

		public InternalInStream(String name) {
			super(name,-1);
		}

		public void setInputStream(InputStream input) {
			this.input = input;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
		}

		@Override
		public int available() throws IOException {
			return input.available();
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
	private final InternalInStream input;

	public DeltaIntegerReader(String name, boolean signed) {
		input = new InternalInStream(name);
		boolean skipCorrupt = false;
		try {
			reader = new RunLengthIntegerReaderV2(input, signed, skipCorrupt);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void setInputStream(InputStream input) {
		this.input.setInputStream(input);
	}
	
	public void read(InputStream input, ReadConsumer read ) throws IOException{
		this.input.setInputStream(input);
		read.read(reader);
	}

	@Deprecated
	public long read() throws IOException {
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
