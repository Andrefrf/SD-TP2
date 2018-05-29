package sys.mapreduce;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import api.storage.BlobStorage;
import utils.Java;

/*
 * Convenience class to resolve a blob name, containing a MapReduce program class, into a program instance.
 * 
 * Expects the name of the class and the blob name to be the same.
 */
final public class Jobs {
	
	static Map<String, String > sources = new HashMap<>();

	
	static protected JobInstance<?> newJobInstance(BlobStorage storage, String jobClassBlob) {
		String source = sources.get( jobClassBlob );
		if( source == null ) {
			StringBuilder sb = new StringBuilder();
			storage.readBlob(jobClassBlob).forEach(line -> sb.append(line).append('\n'));
			sources.put( jobClassBlob, source = sb.toString());
		}
		return new JobInstance<MapReducer<?, ?, ?, ?>>(Java.newInstance(jobClassBlob, source ));
	}
	
	/*
	*
	*
	*/
	static class JobInstance<T extends MapReducer<?, ?, ?, ?>> {
		protected final T instance;
		protected final Type[] kvTypes;

		JobInstance(T instance) {
			this.instance = instance;
			kvTypes = ((ParameterizedType) instance.getClass().getGenericSuperclass()).getActualTypeArguments();
		}

		Type reducerKeyType() {
			return kvTypes[2];
		}

		Type reducerValueType() {
			return kvTypes[3];
		}
	}
	
}
