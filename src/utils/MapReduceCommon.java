package utils;

public class MapReduceCommon {

	public static final Object READ_TIMEOUT = 2000;
	public static final Object CONNECT_TIMEOUT = 2000;

	public static String[] getAddressFromBlockUUID(String block) {
		String[] ret = null;
		
		int idx = block.lastIndexOf("/datanode");
		if (idx > 0) {
			ret = new String[]{block.substring(0, idx+1), block.substring(idx+10)};
		}
		
		return ret;
	}
	
}
