package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class CellData {
	public final long cellId;
	public final long id;
	public final OSMType type;
	public final byte[] bytes;
	public final Path path;
	
	public CellData(long zId, long id, OSMType type, byte[] bytes) {
		this(zId, id, type, bytes, null);
				
	}
	
	public CellData(long zId, long id, OSMType type, byte[] bytes,Path path) {
		this.cellId = zId;
		this.id = id;
		this.type = type;
		this.bytes = bytes;
		this.path = path;
	}

}
