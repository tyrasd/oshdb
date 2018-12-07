package org.heigit.bigspatialdata.oshdb.v0_6.transform.rx;

import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;

public class Block {
	public final long pos;
	public final long id;

	public final OSMType type;
	public final List<Osh> oshs;

	public Block(long pos, long id, OSMType type, List<Osh> oshs) {
		this.pos = pos;
		this.id = id;
		this.type = type;
		this.oshs = oshs;
	}
}
