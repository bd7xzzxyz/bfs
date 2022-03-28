package xyz.bd7xzz.bfs.util;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressUtil {
    private static final byte[] EMPTY = new byte[0];

    /**
     * 压缩
     *
     * @param bytes 源字节数组
     * @return 压缩后的字节数组
     * @throws IOException
     */
    public static byte[] compress(byte[] bytes) throws IOException {
        if (null == bytes || bytes.length == 0) {
            return EMPTY;
        }
        ByteArrayOutputStream bos = null;
        GZIPOutputStream gos = null;
        try {
            bos = new ByteArrayOutputStream();
            gos = new GZIPOutputStream(bos);
            gos.write(bytes);
            return bos.toByteArray();
        } finally {
            if (null != bos) {
                bos.close();
            }
            if (null != gos) {
                gos.close();
            }
        }
    }

    /**
     * 解压缩
     *
     * @param bytes 压缩的字节数组
     * @return 源字节数组
     */
    public static byte[] unCompress(byte[] bytes) throws IOException {
        if (null == bytes || bytes.length == 0) {
            return EMPTY;
        }
        ByteArrayInputStream bis = null;
        ByteArrayOutputStream bos = null;
        GZIPInputStream gis = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            bos = new ByteArrayOutputStream();
            gis = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            return bos.toByteArray();
        } finally {
            if (null != bis) {
                bis.close();
            }
            if (null != bos) {
                bos.close();
            }
            if (null != gis) {
                gis.close();
            }
        }

    }
}
