package org.heigit.bigspatialdata.oshdb.v0_6.util.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.orc.PhysicalWriter.OutputReceiver;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.RunLengthIntegerWriterV2;

public class DeltaIntegerWriter implements OutputReceiver {

	@FunctionalInterface
	public static interface WriterConsumer {
		public void writeTo(IntegerWriter writer) throws IOException;
	}

	private static class InternalOutStream extends PositionedOutputStream {
		private OutputStream out;

		public void setStream(OutputStream out) {
			this.out = out;
		}

		@Override
		public void getPosition(PositionRecorder recorder) throws IOException {
			throw new UnsupportedOperationException("getPosition in InternalOutStream/DeltaIntegerWriter");

		}

		@Override
		public long getBufferSize() {
			throw new UnsupportedOperationException("getBufferSize in InternalOutStream/DeltaIntegerWriter");
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
		}

		@Override
		public void flush() throws IOException {
			out.flush();
		}
	}

	private final IntegerWriter writer;
	private final InternalOutStream output;

	public DeltaIntegerWriter(String name, boolean signed) {
		output = new InternalOutStream();
		writer = new RunLengthIntegerWriterV2(output, signed, true);
	}

	public void write(OutputStream output, WriterConsumer write) throws IOException {
		this.output.setStream(output);
		write.writeTo(writer);
		writer.flush();
	}

	  @Override
	  public void output(ByteBuffer buffer) throws IOException {
	    output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
	        buffer.remaining());
	  }

	  @Override
	  public void suppress() {
	    throw new UnsupportedOperationException("suppress is not implemented for DeltaIntegerWriter");
	  }

}
