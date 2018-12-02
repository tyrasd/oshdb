package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;
import java.util.Objects;

public class OSHDBTag implements Comparable<OSHDBTag>, Serializable {
	private static final long serialVersionUID = 1L;
	private int key;
	private int value;

	public OSHDBTag(int key, int value) {
		this.key = key;
		this.value = value;
	}

	public int getKey() {
		return this.key;
	}

	public int getValue() {
		return this.value;
	}
	
	public void setValue(int value){
		this.value = value;
	}

	public boolean isPresentInKeytables() {
		return this.value >= 0 && this.key >= 0;
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof OSHDBTag && ((OSHDBTag) o).key == this.key && ((OSHDBTag) o).value == this.value;
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.key, this.value);
	}

	@Override
	public String toString() {
		return Integer.toString(this.key) + "=" + Integer.toString(this.value);
	}

	@Override
	public int compareTo(OSHDBTag o) {
		int c = Integer.compare(key, o.key);
		if (c == 0) {
			c = Integer.compare(value, o.value);
		}
		return c;
	}
}
