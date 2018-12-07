package org.heigit.bigspatialdata.oshdb.v0_6.transform.rx;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;

public class BlockOsh {
	public final long pos;
	public final long id;
	public final OSMType type;
	public final Osh osh;
	
	public BlockOsh(long pos, long id, OSMType type, Osh osh) {
		this.pos = pos;
		this.id = id;
		this.type = type;
		this.osh = osh;
	}
	
	
}
