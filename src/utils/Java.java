package utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

/*
 * 
 * An utility class that compiles a Java class, whose source is stored in a String.
 * 
 */
@SuppressWarnings("unchecked")
public class Java {
	private static ByteClassLoader loader = new ByteClassLoader();
	
	synchronized public static <T> Class<T> compile(String className, String source) {
		try {
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

			ClassStorage fileManager = new ClassStorage(compiler.getStandardFileManager(diagnostics, null, null));
			JavaFileObject file = new JavaSourceFromString(className, source);
			CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, Arrays.asList(file));

			boolean success = task.call();
			if (!success)
				diagnostics.getDiagnostics().forEach(d -> {
					System.err.println(d.getMessage(null));
				});
			
			if (success ) 
				loader.addAll( fileManager.classes );			
			return (Class<T>) loader.findClass(className);
		} catch (Exception x) {
			x.printStackTrace();
		}
		return null;
	}
	
	public static <T> T newInstance( String className ) {
		return newInstance(className, "");
	}

	public static <T> T newInstance( String className, String source ) {
		try {
			Class<T> _class = (Class<T>)loader.findClass(className);
			if( _class == null )
				_class = compile( className, source );
			return _class.newInstance() ;
		} catch (Exception x) {
			x.printStackTrace();
			return null;
		}
	}

	static class ClassStorage extends ForwardingJavaFileManager<JavaFileManager> {
		Map<String, ByteArrayJavaClass> classes = new HashMap<>();

		ClassStorage(JavaFileManager fileManager) {
			super(fileManager);
		}

		@Override
		public JavaFileObject getJavaFileForOutput(Location location, final String className, Kind kind,
				FileObject sibling) throws IOException {
			if (kind.equals(Kind.CLASS)) {
				ByteArrayJavaClass res = new ByteArrayJavaClass(className);
				classes.put(className, res);
				return res;
			} else
				throw new IOException("Unexpected kind...");
		}
	}

	static class JavaSourceFromString extends SimpleJavaFileObject {
		final String code;

		JavaSourceFromString(String name, String code) {
			super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
			this.code = code;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
			return code;
		}
	}

	static class ByteArrayJavaClass extends SimpleJavaFileObject {
		private ByteArrayOutputStream stream;

		ByteArrayJavaClass(String name) {
			super(URI.create("bytes:///" + name), Kind.CLASS);
			stream = new ByteArrayOutputStream();
		}

		@Override
		public OutputStream openOutputStream() throws IOException {
			return stream;
		}

		public byte[] getBytes() {
			return stream.toByteArray();
		}
	}

	static class ByteClassLoader extends ClassLoader {
		private final Map<String, ByteArrayJavaClass> extraClassDefs = new HashMap<>();
		private final Map<String, Class<?>> definedClasses = new HashMap<>();

		void addAll( Map<String, ByteArrayJavaClass> extraClassDefs ) {
			this.extraClassDefs.putAll( extraClassDefs ) ;			
		}
		
		@Override
		protected Class<?> findClass(final String name) {
			Class<?> _class = definedClasses.get(name);
			if( _class == null ) {
				ByteArrayJavaClass bajc = this.extraClassDefs.remove(name);
				if (bajc != null) {
					byte[] classBytes = bajc.getBytes();
					_class = defineClass(name, classBytes, 0, classBytes.length);
					definedClasses.put(name, _class);
				}
			}
			return _class;
		}
	}

}