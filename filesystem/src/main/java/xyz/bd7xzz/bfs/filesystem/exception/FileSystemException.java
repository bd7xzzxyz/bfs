package xyz.bd7xzz.bfs.filesystem.exception;

public class FileSystemException extends RuntimeException {
    private String message;
    private int code;

    public FileSystemException(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
