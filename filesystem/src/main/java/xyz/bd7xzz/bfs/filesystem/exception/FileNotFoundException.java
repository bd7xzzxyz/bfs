package xyz.bd7xzz.bfs.filesystem.exception;

public class FileNotFoundException extends RuntimeException {
    private String message;

    public FileNotFoundException(long fd) {
        this.message = String.format("file(fd=%d) cannot found", fd);
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
