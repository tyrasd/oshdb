package org.heigit.bigspatialdata.oshdb.v0_6.osh;
import java.io.IOException;

import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

public class Playground {

		
	public static void main(String[] args) throws IOException {
		long min = OSHDB.VALID_MIN_LONGITUDE;
		long max = OSHDB.VALID_MAX_LONGITUDE;
		
		long minL = min & 0xffffffffL;
		System.out.printf("%64s(%d)%n%64s(%d)%n",Long.toBinaryString(min),min,Long.toBinaryString(minL),minL);
		
		long maxL = max & 0xffffffffL;
		System.out.printf("%64s(%d)%n%64s(%d)%n",Long.toBinaryString(max),max,Long.toBinaryString(maxL),maxL);
		
		System.out.printf("%64s(%d)%n%64s(%d)%n",Long.toBinaryString(1800000000L),1800000000L,Long.toBinaryString(3600000000L),3600000000L);
		
		System.out.println(max-min);
		System.out.println(maxL+3600000000L);
		System.out.println(Long.numberOfLeadingZeros(3600000000L));
				
	}

}
