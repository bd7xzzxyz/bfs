package xyz.bd7xzz.bfs.filesystem.struct;

import xyz.bd7xzz.bfs.util.SerializeUtil;

import java.nio.ByteBuffer;

public class FileBlock {
    private long fd;
    private byte[] hex;
    private long size;

    public FileBlock(long fd, byte[] hex, long size) {
        this.fd = fd;
        this.hex = hex;
        this.size = size;
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

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    /**
     * 转换属性到字节数组
     *
     * @return 字节数组
     */
    public byte[] toByte() {
        byte[] fdBytes = SerializeUtil.longToByte(fd);
        byte[] sizeBytes = SerializeUtil.longToByte(size);
        return ByteBuffer.allocate(fdBytes.length + hex.length + sizeBytes.length).put(fdBytes).put(hex).put(sizeBytes).array();
    }

    /**
     * 对象字节长度
     *
     * @return 长度
     */
    public static int size() {
        return 8  * 2 + 32;
    }
}
