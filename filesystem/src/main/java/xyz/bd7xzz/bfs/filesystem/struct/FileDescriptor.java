package xyz.bd7xzz.bfs.filesystem.struct;

import java.util.List;

public class FileDescriptor {
    private long fd;
    private byte[] hex;
    private List<FileBlock> fileBlocks;

    public FileDescriptor(long fd, byte[] hex, List<FileBlock> fileBlocks) {
        this.fd = fd;
        this.hex = hex;
        this.fileBlocks = fileBlocks;
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

    public List<FileBlock> getFileBlocks() {
        return fileBlocks;
    }

    public void setFileBlocks(List<FileBlock> fileBlocks) {
        this.fileBlocks = fileBlocks;
    }
}
