package xyz.bd7xzz.bfs.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SerializeUtil {
    private static final String EMPTY = "";

    /**
     * long转字节数组
     *
     * @param data 原始数据
     * @return 字节数组
     */
    public static byte[] longToByte(long data) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(data).array();
    }

    /**
     * string转字节数组
     *
     * @param data 原始数据
     * @return 字节数组
     */
    public static byte[] stringToByte(String data) {
        return Optional.ofNullable(data).orElse(EMPTY).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * int转字节数组
     *
     * @param data 原始数据
     * @return 字节数组
     */
    public static byte[] intToByte(int data) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(data).array();
    }
}
