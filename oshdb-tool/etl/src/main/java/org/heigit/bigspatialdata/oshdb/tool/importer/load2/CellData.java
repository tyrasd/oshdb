package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class CellData {
	public final long cellId;
	public final long id;
	public final OSMType type;
	public final byte[] bytes;
	
	
	public CellData(long zId, long id, OSMType type, byte[] bytes) {
		this.cellId = zId;
		this.id = id;
		this.type = type;
		this.bytes = bytes;
	}

}
