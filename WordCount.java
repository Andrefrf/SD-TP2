import sys.mapreduce.MapReducer;

public class WordCount extends MapReducer<String, String, String, Long>{

	@Override
	public void map(String blob, String line ) {
		for( String word : line.split("(?U)[\\p{Space}\\p{Punct}]"))
			yield( word.toLowerCase(), 1L);
	}
	
	@Override
	public void reduce( String word, Iterable<Long> values ) {
		long sum = 0;
		for( long v : values )
			sum += v;
		yield( word, sum );
	}
}
