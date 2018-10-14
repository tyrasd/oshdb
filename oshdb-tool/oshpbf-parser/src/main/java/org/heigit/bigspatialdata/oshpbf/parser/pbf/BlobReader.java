package org.heigit.bigspatialdata.oshpbf.parser.pbf;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.NoSuchElementException;

import org.heigit.bigspatialdata.oshpbf.parser.util.ByteBufferBackedInputStream;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.io.MoreFiles;

import crosby.binary.Fileformat;

public class BlobReader implements PeekingIterator<PbfBlob>, Closeable {
	private static final byte[] SIGNATURE_OSMDATA = //
			{ /* wire_type */10, /* stringSize */7, 79, 83, 77, 68, 97, 116, 97 };
	private static final byte[] SIGNATURE_OSMHEADER = //
			{ /* wire_type */10, /* stringSize */9, 79, 83, 77, 72, 101, 97, 100, 101, 114 };
	private static final int BLOBHEADER_SIZE_BYTES = 4;
	private static final int SIGNATURE_SIZE_BYTES = Math.max(SIGNATURE_OSMDATA.length, SIGNATURE_OSMHEADER.length);

	private static long headerPos;
	private PbfBlob next = null;
	private Exception checkedException = null;

	private final CountingInputStream input;
	private final long limit;
	private boolean skipFirst = false;
	private boolean aligned = false;
	private long blobId = 0;

	public static BlobReader newInstance(Path pbf, long start, long softLimit, long hardLimit, boolean skipFirst)
			throws IOException {
		ByteSource source = MoreFiles.asByteSource(pbf, StandardOpenOption.READ);
		InputStream input = source.openBufferedStream();
		return newInstance(input, start, softLimit, hardLimit, skipFirst);
	}

	public static BlobReader newInstance(InputStream input, long start, long softLimit, long hardLimit,
			boolean skipFirst) throws IOException {
		CountingInputStream countingInput = new CountingInputStream(ByteStreams.limit(input,hardLimit));
		countingInput.skip(start);
		return new BlobReader(countingInput, softLimit, skipFirst);
	}

	private BlobReader(CountingInputStream input, long limit, boolean skipFirst) {
		this.input = input;
		this.limit = limit;
		this.skipFirst = skipFirst;
	}

	@Override
	public boolean hasNext() {
		try {
			return (next != null) || ((next = getNext()) != null);
		} catch (Exception e) {
			checkedException = e;
		}
		return false;
	}

	public boolean wasException() {
		return checkedException != null;
	}

	public Exception getException() {
		return checkedException;
	}

	@Override
	public PbfBlob peek() {
		if (!hasNext())
			throw new NoSuchElementException();
		return next;
	}

	@Override
	public PbfBlob next() {
		if (!hasNext())
			throw new NoSuchElementException();
		PbfBlob ret = next;
		next = null;
		return ret;
	}
	
	@Override
	public void close() throws IOException {
		input.close();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove");
	}

	protected PbfBlob getNext() throws Exception {
		if (input.available() > 0 && input.getCount() < limit ) {
			return readBlob();
		}
		return null;
	}

	private PbfBlob readBlob() throws IOException {
		Fileformat.BlobHeader header;

		if (!aligned) {
			long pos = input.getCount();
			header = seekBlobHeaderStart(ByteStreams.limit(input, limit - input.getCount()));
			if (header == null)
				return null;
			headerPos = headerPos + pos;
			aligned = true;
		} else {
			long pos = input.getCount();
			header = readBlobHeader(input);
			if (header == null)
				return null;
			headerPos = pos;
		}

		byte[] buffer = new byte[header.getDatasize()];
		long blobPos = input.getCount();
		readFully(input, buffer, 0, buffer.length);
		final Fileformat.Blob blob = Fileformat.Blob.PARSER.parseFrom(buffer);
		PbfBlob pbfBlob = new PbfBlob(blobId++,headerPos, header, blob, skipFirst, blobPos >= limit);
		skipFirst = false;
		// blob.setHeader(header);
		return pbfBlob;
	}

	private static Fileformat.BlobHeader readBlobHeader(InputStream in) throws IOException {
		int headerLength = readIntNetworkByteOrder(in);
		byte[] buffer = new byte[headerLength];
		readFully(in, buffer, 0, buffer.length);
		return Fileformat.BlobHeader.PARSER.parseFrom(buffer);
	}

	public static Fileformat.BlobHeader seekBlobHeaderStart(final InputStream in) throws IOException {
		final byte[] pushBackBytes = new byte[BLOBHEADER_SIZE_BYTES + SIGNATURE_SIZE_BYTES];
		long pos = 0;
		int pbi = 0;
		for (int i = 0; i < BLOBHEADER_SIZE_BYTES; i++) {
			pushBackBytes[pbi++] = (byte) in.read();
			pos++;
		}

		int nextByte;
		final List<S> signatures = Lists.newArrayList(S.of(SIGNATURE_OSMDATA), S.of(SIGNATURE_OSMHEADER));
		while ((nextByte = in.read()) != -1) {
			pushBackBytes[pbi++] = (byte) nextByte;
			pos++;
			for (S s : signatures) {
				if (s.test(nextByte)) {
					int length = BLOBHEADER_SIZE_BYTES + s.length();
					pos -= length;
					final PushbackInputStream pushBackStream = new PushbackInputStream(in, length);
					for (int i = 0; i < length; i++) {
						pushBackStream.unread(pushBackBytes[--pbi]);
						if (pbi == 0)
							pbi = pushBackBytes.length;
					}
					Fileformat.BlobHeader blobHeader = readBlobHeader(pushBackStream);
					headerPos = pos;
					return blobHeader;
				}
			}

			//
			if (pbi == pushBackBytes.length) {
				pbi = 0;
			}
			if (in.available() == 0)
				break;
		}
		return null;
	}

	private static void readFully(InputStream in, byte b[], int off, int len) throws IOException {
		if (len < 0)
			throw new IndexOutOfBoundsException();
		int n = 0;
		while (n < len) {
			int count = in.read(b, off + n, len - n);
			if (count < 0)
				throw new EOFException();
			n += count;
		}
	}

	private static int readIntNetworkByteOrder(InputStream in) throws IOException {
		int ch1 = in.read();
		int ch2 = in.read();
		int ch3 = in.read();
		int ch4 = in.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	private static class S {
		private final byte[] signature;
		private int pos = 0;

		private S(byte[] signature) {
			this.signature = signature;
		}

		public static S of(byte[] signature) {
			return new S(signature);
		}

		public boolean test(int b) {
			if (b == signature[pos]) {
				pos++;
				if (pos == signature.length)
					return true;
			} else {
				pos = 0;
			}

			return false;
		}

		public int length() {
			return signature.length;
		}
	}
}
