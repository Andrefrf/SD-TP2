package sys.mapreduce;

import api.mapreduce.MapReduce;

abstract public class MapReducer<MK, MV, RK, RV>  implements MapReduce<MK, MV, RK, RV>  {
		
	@Override
	public void map_init() {}
	
	@Override
	public void map(MK key, MV val) {
		yield( key, val );
	}
	
	@Override
	public void map_end() {		
	}
	
	@Override
	public void reduce_init() {		
	}
	
	@Override
	public void reduce(RK key, Iterable<RV> values) {
		Thread.dumpStack();
		values.forEach( value -> yield( key, value ));
	}

	@Override
	public void reduce_end() {		
	}
		
	
	final protected <K, V> void yield(K key, V val) {
		yielder.yield(key, val);
	}
	
	final void setYielder( Yielder yielder ) {
		this.yielder = yielder;
	}

	
	interface Yielder {
		void yield( Object key, Object val );
	}
	
	private Yielder yielder;
	
}
