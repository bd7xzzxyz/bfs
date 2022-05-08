package xyz.bd7xzz.bfs.filesystem.struct;

public class FileBlock {
    private long fd;
    private byte[] hex;

    public FileBlock(long fd, byte[] hex) {
        this.fd = fd;
        this.hex = hex;
    }

    public long getFd() {
        return fd;
    }

    public void setFd(long fd) {
        this.fd = fd;
    }

    public byte[] getHex() {
        return hex;
    }

    public void setHex(byte[] hex) {
        this.hex = hex;
    }

}
