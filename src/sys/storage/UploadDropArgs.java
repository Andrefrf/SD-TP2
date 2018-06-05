package sys.storage;

public class UploadDropArgs {
    private final String path;	@SuppressWarnings("unused")

    private final String mode;	@SuppressWarnings("unused")

    private final boolean autorename;	@SuppressWarnings("unused")

    private final boolean mute;	@SuppressWarnings("unused")

    public UploadDropArgs(String path) {
        this.path = path;
        this.mode = "add";
        this.autorename = true;
        this.mute = false;
    }
}