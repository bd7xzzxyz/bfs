package xyz.bd7xzz.bfs.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtil {
    /**
     * 求md5
     * @param bytes 原始数据字节数组
     * @return hash字节数组
     * @throws NoSuchAlgorithmException
     */
    public static byte[] hex(byte []bytes) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(bytes);
        return digest.digest();
    }
}
