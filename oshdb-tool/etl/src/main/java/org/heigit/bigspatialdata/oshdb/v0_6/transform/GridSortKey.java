package org.heigit.bigspatialdata.oshdb.v0_6.transform;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.OSHGridSort.SortKey;

public class GridSortKey implements SortKey<GridSortKey> {
	private final long zid;
	private final long id;
	private final long pos;
			
	
	public GridSortKey(long zid, long id, long pos) {
		this.zid = zid;
		this.id = id;
		this.pos = pos;
	}

	@Override
	public int compareTo(GridSortKey o) {
		int c = ZGrid.ORDER_DFS_BOTTOM_UP.compare(zid, o.zid);
		if(c == 0){
			c = Long.compare(id, o.id);
		}
			
		return c;
	}

	@Override
	public long sortKey() {
		return zid;
	}

	@Override
	public long oshId() {
		return id;
	}

	@Override
	public long pos() {
		return pos;
	}
	
}
