package xyz.bd7xzz.bfs.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SerializeUtil {
    private static final String EMPTY = "";
    private static final byte[] EMPTY_BYTES = new byte[0];

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

    /**
     * 字节数组转long
     *
     * @param bytes 字节数组
     * @return long
     */
    public static long byteToLong(byte[] bytes) {
        return ByteBuffer.allocate(Long.SIZE).put(bytes).getLong();
    }

    /**
     * 对象转换字节数组
     *
     * @param obj 对象
     * @param <T> 类型
     * @return 字节数组
     */
    public static <T> byte[] objectToByte(T obj) {
        if (null == obj) {
            return EMPTY_BYTES;
        }
        Kryo kryo = new Kryo();
        kryo.register(obj.getClass());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        try {
            kryo.writeObject(output, obj);
            return bos.toByteArray();
        } finally {
            output.close();
            try {
                bos.close();
            } catch (Exception e) {
                //ignored
            }
        }
    }

    /**
     * 字节数组转换成对象
     *
     * @param clazz 类
     * @param bytes 字节数组
     * @param <T>   类型
     * @return 原始对象
     */
    public static <T> T byteToObject(Class<T> clazz, byte[] bytes) {
        if (null == bytes || bytes.length == 0 || null == clazz) {
            return null;
        }
        Kryo kryo = new Kryo();
        kryo.register(clazz);
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        Input input = new Input(bis);
        try {
            return kryo.readObject(input, clazz);
        } finally {
            input.close();
            try {
                bis.close();
            } catch (Exception e) {
                //ignored
            }
        }
    }
}
