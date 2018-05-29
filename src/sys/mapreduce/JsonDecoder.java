package sys.mapreduce;

import java.lang.reflect.Type;
import java.util.Iterator;

import utils.JSON;

/*
 * Wraps an iterator (and BlobReader) of strings, each containing a single json object, 
 * into an iterator of objects of the given type, 
 * 
 */
final public class JsonDecoder implements Iterator<Object> {

	final Iterator<String> it;
	final Type decodedType;
	
	JsonDecoder(Iterator<String> it, Type decodedType ) {
		this.it = it;
		this.decodedType = decodedType;
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public Object next() {
		return JSON.decode( it.next(), decodedType );
	}
}
