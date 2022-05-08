package xyz.bd7xzz.bfs.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtil {
    /**
     * 求md5
     *
     * @param bytes 原始数据字节数组
     * @return hash字节数组
     * @throws NoSuchAlgorithmException
     */
    public static byte[] hex(byte[] bytes) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(bytes);
        return digest.digest();
    }

    /**
     * 比对俩个hash是否相同
     *
     * @param hex1
     * @param hex2
     * @return tre相同 false不相同
     */
    public static boolean hexEquals(byte[] hex1, byte[] hex2) {
        if (hex1 == hex2) {
            return true;
        }
        if (hex1 != null && hex2 != null && hex1.length == hex2.length) {
            for (int i = 0; i < hex1.length; i++) {
                if (hex1[i] != hex2[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
