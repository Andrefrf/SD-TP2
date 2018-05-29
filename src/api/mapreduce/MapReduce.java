package api.mapreduce;

public interface MapReduce<MK, MV, RK, RV> {

		void map_init();
		
		void map(MK key, MV val);
		
		void map_end();
		
		void reduce_init();
		
		void reduce(RK key, Iterable<RV> val);
		
		void reduce_end();
}
