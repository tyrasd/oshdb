package org.heigit.bigspatialdata.oshpbf.parser.pbf;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;



import crosby.binary.Fileformat;

public class BlobReader implements PeekingIterator<PbfBlob>, Closeable {
	private static final byte[] SIGNATURE_OSMDATA = //
			{ /* wire_type */10, /* stringSize */7, 79, 83, 77, 68, 97, 116, 97 };
	private static final byte[] SIGNATURE_OSMHEADER = //
			{ /* wire_type */10, /* stringSize */9, 79, 83, 77, 72, 101, 97, 100, 101, 114 };
	private static final int BLOBHEADER_SIZE_BYTES = 4;
	private static final int SIGNATURE_SIZE_BYTES = Math.max(SIGNATURE_OSMDATA.length, SIGNATURE_OSMHEADER.length);

	private PbfBlob next = null;
	private Exception checkedException = null;

	private final DataInputStream input;
	private long pos = 0;
	private long headerPos = 0;
	private final long softLimit;
	private final long hardLimit;
	private boolean skipFirst = false;
	private boolean aligned = false;
	private long blobId = 0;
	
	public static BlobReader newInstance(Path pbf, long start, long softLimit, long hardLimit, boolean skipFirst)
			throws IOException {
		RandomAccessFile raf = new RandomAccessFile(pbf.toFile(), "r");
		FileChannel channel = raf.getChannel().position(start);
		DataInputStream input = new DataInputStream(Channels.newInputStream(channel));
		return new BlobReader(input,start,softLimit,hardLimit,skipFirst);
	}


	private BlobReader(DataInputStream input, long start, long softLimit, long hardLimit, boolean skipFirst) throws IOException {
		this.input = input;
		this.pos = start;
		this.softLimit = softLimit;
		this.hardLimit = hardLimit;
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
		if (input.available() > 0 &&  headerPos < softLimit && pos < hardLimit ) {
			//System.out.printf("BlobReader: readBlob hp:%d < sf:%d && p:%d < hl:%d input.availiable:%d >>%n",headerPos,softLimit,pos,hardLimit,input.available());
			PbfBlob blob = readBlob();
			//System.out.printf("<< hp:%d, p:%d bid:%d%n",headerPos,pos,blobId);
			return blob;
			
		}
		return null;
	}

	private PbfBlob readBlob() throws IOException {
		Fileformat.BlobHeader header;
		if (!aligned) {
			header = seekBlobHeaderStart();
			if (header == null)
				return null;
			aligned = true;
		} else {
			headerPos = pos;
			header = readBlobHeader(input);
			if (header == null)
				return null;
		}

		byte[] buffer = new byte[header.getDatasize()];
		input.readFully(buffer);
		pos += buffer.length;
		
		final Fileformat.Blob blob = Fileformat.Blob.PARSER.parseFrom(buffer);
		PbfBlob pbfBlob = new PbfBlob(blobId++,headerPos, header, blob, skipFirst, headerPos >= softLimit);
		skipFirst = false;
		return pbfBlob;
	}

	private Fileformat.BlobHeader readBlobHeader(DataInputStream input) throws IOException {
		int headerLength = input.readInt();
		pos += 4;
		byte[] buffer = new byte[headerLength];
		input.readFully(buffer);
		pos += buffer.length;
		return Fileformat.BlobHeader.PARSER.parseFrom(buffer);
	}
	
	public Fileformat.BlobHeader seekBlobHeaderStart() throws IOException {		
		final byte[] pushBackBytes = new byte[BLOBHEADER_SIZE_BYTES + SIGNATURE_SIZE_BYTES];
		int pbi = 0;
		for (int i = 0; i < BLOBHEADER_SIZE_BYTES; i++) {
			pushBackBytes[pbi++] = input.readByte();
			pos++;
		}

		int nextByte;
		final List<S> signatures = Lists.newArrayList(S.of(SIGNATURE_OSMDATA), S.of(SIGNATURE_OSMHEADER));
		while (pos < softLimit && ((nextByte = input.read()) != -1)){
			pushBackBytes[pbi++] = (byte)nextByte;
			pos++;
			for (S s : signatures) {
				if (s.test(nextByte)) {
					int length = BLOBHEADER_SIZE_BYTES + s.length();
					pos -= length;
					final PushbackInputStream pushBackStream = new PushbackInputStream(input, length);
					for (int i = 0; i < length; i++) {
						pushBackStream.unread(pushBackBytes[--pbi]);
						if (pbi == 0)
							pbi = pushBackBytes.length;
					}
					headerPos = pos;
					Fileformat.BlobHeader blobHeader = readBlobHeader(new DataInputStream(pushBackStream));
					return blobHeader;
				}
			}

			//
			if (pbi == pushBackBytes.length) {
				pbi = 0;
			}
			if (input.available() == 0)
				break;
		}
		return null;
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
