package xyz.bd7xzz.bfs.filesystem.struct;

import xyz.bd7xzz.bfs.util.SerializeUtil;

import java.nio.ByteBuffer;
import java.util.List;

public class FileDescriptor {
    private long fd;
    private byte[] hex;
    private List<FileBlock> fileBlocks;
    private long size;
    private String physicalPath;
    private String fileName;

    private FileDescriptor(Builder builder) {
        this.fd = builder.fd;
        this.hex = builder.hex;
        this.fileBlocks = builder.fileBlocks;
        this.size = builder.size;
        this.physicalPath = builder.physicalPath;
        this.fileName = builder.fileName;
    }

    public static class Builder {
        private long fd;
        private byte[] hex;
        private List<FileBlock> fileBlocks;
        private long size;
        private String physicalPath;
        private String fileName;

        public FileDescriptor build() {
            return new FileDescriptor(this);
        }

        public Builder fd(long fd) {
            this.fd = fd;
            return this;
        }

        public Builder hex(byte[] hex) {
            this.hex = hex;
            return this;
        }

        public Builder fileBlocks(List<FileBlock> fileBlocks) {
            this.fileBlocks = fileBlocks;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder physicalPath(String physicalPath) {
            this.physicalPath = physicalPath;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }
    }

    public static FileDescriptor.Builder newBuilder() {
        return new FileDescriptor.Builder();
    }

    public long getFd() {
        return fd;
    }

    public byte[] getHex() {
        return hex;
    }

    public List<FileBlock> getFileBlocks() {
        return fileBlocks;
    }

    public long getSize() {
        return size;
    }

    public String getPhysicalPath() {
        return physicalPath;
    }

    public String getFileName() {
        return fileName;
    }

    public byte[] toByte() {
        byte[] fdBytes = SerializeUtil.longToByte(fd);
        byte[] sizeBytes = SerializeUtil.longToByte(size);
        int blockSize = FileBlock.size() * fileBlocks.size();
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
        for (FileBlock fileBlock : fileBlocks) {
            byteBuffer.put(fileBlock.toByte());
        }
        byte[] blockBytes = byteBuffer.array();
        byte[] physicalPathBytes = SerializeUtil.stringToByte(physicalPath);
        byte[] fileNameBytes = SerializeUtil.stringToByte(fileName);

        return ByteBuffer.allocate(fdBytes.length + sizeBytes.length + 4 + blockBytes.length + physicalPathBytes.length + fileNameBytes.length)
                .put(fdBytes)
                .put(sizeBytes)
                .put(SerializeUtil.intToByte(blockSize))
                .put(blockBytes)
                .put(physicalPathBytes)
                .put(fileNameBytes)
                .array();
    }

    public static FileDescriptor parseByte(byte[] bytes) {
        if (null == bytes || bytes.length == 0) {
            return null;
        }
        byte[] longTmp = new byte[8];
        System.arraycopy(bytes, 0, longTmp, 0, 8);
        long fd = SerializeUtil.byteToLong(longTmp);
        byte[] hexTmp = new byte[32];
        System.arraycopy(bytes, 8, hexTmp, 0, 32);
        return FileDescriptor.newBuilder()
                .fd(fd)
                .hex(hexTmp)
                .build();
    }

}
