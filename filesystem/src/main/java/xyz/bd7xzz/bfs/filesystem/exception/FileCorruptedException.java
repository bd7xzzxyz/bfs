package xyz.bd7xzz.bfs.filesystem.exception;

public class FileCorruptedException extends RuntimeException {
    private String message;

    public FileCorruptedException(long fd, long blockIndex) {
        this.message = String.format("file(fd=%d block=%d) was corrupted", fd, blockIndex);
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
