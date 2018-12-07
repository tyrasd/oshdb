package org.heigit.bigspatialdata.oshdb.v0_6;

import java.util.Comparator;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class OSHDB {

	public static final long GEOM_PRECISION_TO_LONG = 10000000L;
	public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;

	public static long doubleToLong(double x) {
		return (long) (x * GEOM_PRECISION_TO_LONG);
	}

	public static double longToDouble(long x) {
		return (double) ((double) x * GEOM_PRECISION);
	}
	
	public static final int MAX_ZOOM = 15;
	
	public static final long VALID_MIN_LONGITUDE = doubleToLong(-180.0);
	public static final long VALID_MIN_LATITUDE = doubleToLong(-90.0);
	public static final long VALID_MAX_LONGITUDE = doubleToLong(180.0);
	public static final long VALID_MAX_LATITUDE = doubleToLong(90.0);

	public static final int OSH_HEADER_SINGLE   = 1 << 7;// 0b10000000; single osh entity
	public static final int OSH_HEADER_VISIBLE  = 1 << 6;// 0b01000000; still visible
	public static final int OSH_HEADER_INVALID  = 1 << 5;// 0b00100000;
	public static final int OSH_HEADER_HAS_TAGS = 1 << 4;// 0b00010000; has tags
	
	public static final int OSH_HEADER_NODE_POINT         = 1 << 3;// 0b00001000;
	public static final int OSH_HEADER_NODE_BACKREF_WAY   = 1 << 2; //
	public static final int OSH_HEADER_NODE_BACKREF_REL   = 1 << 1; //
	
	public static final int OSH_HEADER_REL_BACKREF_REL	   = 1 << 3;
	public static final int OSH_HEADER_REL_DUP1 = 1 << 2;
	public static final int OSH_HEADER_REL_DUP2 = 1 << 1;
	
	
	

	public static final int OSM_HEADER_END = 1 << 7;// 0b10000000;
	public static final int OSM_HEADER_VISIBLE = 1 << 6;// 0b01000000;
	public static final int OSM_HEADER_MISSING = 1 << 5;// 0b00100000;
	public static final int OSM_HEADER_CHG_TAGS = 1 << 4;// 0b00010000;
	public static final int OSM_HEADER_CHG_UID = 1 << 3;// 0b00001000;
	public static final int OSM_HEADER_CHG_CS = 1 << 2;// 0b00000100;
	public static final int OSM_HEADER_CHG_EXT = 1 << 1;// 0b00000010;
	public static final int OSM_HEADER_UNUSED = 1 << 0;// 0b00000001;

	public static final int ACTION_LENGTH_BITSIZE = 6;
	public static final int ACTION_LENGTH_BITMASK = 0x3f; // or 0b111111

	public static final int ACTION_TAKE = 0 << 6;// 0b00000000;
	public static final int ACTION_SKIP = 1 << 6;// 0b01000000;
	public static final int ACTION_ADD = 2 << 6;// 0b10000000;
	public static final int ACTION_UPDATE = 3 << 6;// 0b11000000;
													// (LCS_ACTION_SKIP |
													// LCS_ACTION_ADD);

	public enum SortOrder {
		ASC(1), DESC(-1);

		public final int dir;
		public final Comparator<OSMEntity> compare;

		SortOrder(int dir) {
			this.dir = dir;
			this.compare = (a, b) -> {
				int c = Integer.compare(a.getVersion(), b.getVersion());
				if (c == 0) {
					c = a.getTimestamp().compareTo(b.getTimestamp());
				}
				return c * dir;
			};
		}

	}

	public static final SortOrder sortOrder = SortOrder.DESC;
}
