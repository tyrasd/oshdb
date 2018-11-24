package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.Closeable;
import java.io.DataInputStream;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import com.google.common.io.MoreFiles;

public class ReaderBitmap implements Closeable, Iterator<CellBitmaps> {
	private final Path path;
	private final DataInputStream data;
	private CellBitmaps next = null;
	private Exception exception = null;

	public ReaderBitmap(Path path) throws IOException {
		this.path = path;
		this.data = new DataInputStream(MoreFiles.asByteSource(path).openBufferedStream());
	}

	@Override
	public boolean hasNext() {
		try {
			return (next != null) || ((next = getNext()) != null);
		} catch (IOException e) {
			this.exception = e;
			return false;
		}
	}

	@Override
	public CellBitmaps next() {
		if (!hasNext())
			throw new NoSuchElementException();
		CellBitmaps ret = next;
		next = null;
		return ret;
	}

	@Override
	public void close() throws IOException {
		data.close();
	}

	private CellBitmaps getNext() throws IOException {
		if (data.available() == 0) {
			return null;
		}

		final long zId = data.readLong();
		final long totalSizeBytes = data.readLong();
		final byte flag = data.readByte();
		final Roaring64NavigableMap bitmapNodes;
		if ((flag & 1) == 1) {
			bitmapNodes = Roaring64NavigableMap.bitmapOf();
			bitmapNodes.deserialize(data);
		} else {
			bitmapNodes = Roaring64NavigableMap.bitmapOf();
		}

		final long sizeWays = data.readLong();
		final Roaring64NavigableMap bitmapWays;
		if ((flag & 2) == 2) {
			bitmapWays = Roaring64NavigableMap.bitmapOf();
			bitmapWays.deserialize(data);
		} else {
			bitmapWays = Roaring64NavigableMap.bitmapOf();
		}

		final CellBitmaps cell = new CellBitmaps(path, zId, bitmapNodes, bitmapWays);
		return cell;
	}
}
