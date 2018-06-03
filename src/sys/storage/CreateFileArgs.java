package sys.storage;

public class CreateFileArgs {
	final String path;
	final String mode;
	final boolean autorename;
	final boolean mute;
	
	public CreateFileArgs(String path) {
		this.path = path;
		this.mode = "add";
		this.autorename = false;
		this.mute = false;
	}
}
