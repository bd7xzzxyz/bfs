package xyz.bd7xzz.bfs.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtil {

    /**
     * 检查文件是否存在
     *
     * @param filePath 文件绝对路径
     * @return true存在 false不存在
     */
    public static boolean exists(String filePath) {
        return new File(filePath).exists();
    }

    /**
     * 创建目录
     *
     * @param filePath       目录绝对路径
     * @param deleteOnExists 已存在是否删除
     */
    public static void mkdir(String filePath, boolean deleteOnExists) {
        File folder = new File(filePath);
        if (folder.exists() && deleteOnExists) {
            folder.deleteOnExit();
        }
        boolean res = folder.mkdirs();
        if (!res) {
            throw new RuntimeException(filePath + " create filed");
        }
    }


    /**
     * 写文件
     *
     * @param filePath 文件绝对路径
     * @param bytes    文件字节数组
     * @param append   是否追加写
     * @param fsync    是否同步刷盘
     * @throws IOException
     */
    public static void writeFile(String filePath, byte[] bytes, boolean append, boolean fsync) throws IOException {
        File file = new File(filePath);
        FileOutputStream outputStream = null;
        FileChannel channel = null;
        try {
            outputStream = new FileOutputStream(file, append);
            channel = outputStream.getChannel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            channel.write(byteBuffer);
            if (fsync) {
                outputStream.getFD().sync();
            }
        } finally {
            if (null != channel) {
                channel.close();
            }
            if (null != outputStream) {
                outputStream.close();
            }
        }
    }

    /**
     * 读取文件
     *
     * @param filePath
     * @return
     */
    public static byte[] readFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new RuntimeException("invalid " + filePath);
        }
        FileChannel channel = null;
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            channel = inputStream.getChannel();
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size());
            while ((channel.read(byteBuffer)) > 0) {
                //nothing
            }
            return byteBuffer.array();
        } finally {
            if (null != channel) {
                channel.close();
            }
            if (null != inputStream) {
                inputStream.close();
            }
        }
    }
}
