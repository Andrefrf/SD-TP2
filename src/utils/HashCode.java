package utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class HashCode {
		static HashFunction hf = Hashing.murmur3_32();
		
		static int of(String str) {
			return hf.hashBytes( str.getBytes() ).asInt();
		}
}
