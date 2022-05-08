package xyz.bd7xzz.bfs.filesystem;

import xyz.bd7xzz.bfs.filesystem.struct.FileMode;

public interface VFSOperator {

    /**
     * 初始化
     */
    void init();

    /**
     * 创建vfs
     *
     * @param namespace 命名空间
     */
    void mkVFS(String namespace);

    /**
     * 销毁vfs
     *
     * @param namespace 命名空间
     */
    void destroyVFS(String namespace);

    /**
     * 获取物理路径
     *
     * @param namespace 命名空间
     * @param path      vfs相对路径
     * @return 物理路径
     */
    String getPhysicalPath(String namespace, String path);

    /**
     * 写入文件（指定写入模式）
     *
     * @param bytes     字节流
     * @param fileMode  写入模式
     * @param namespace 命名空间
     * @param path      写入的vfs路径
     * @param fileName  原始文件名
     * @return 文件描述符
     */
    long write(byte[] bytes, FileMode fileMode, String namespace, String path, String fileName);

    /**
     * 写入文件（创建新文件）
     *
     * @param bytes     字节流
     * @param namespace 命名空间
     * @param path      写入的vfs路径
     * @param fileName  原始文件名
     * @return 文件描述符
     */
    long write(byte[] bytes, String namespace, String path, String fileName);

    /**
     * 读取文件
     *
     * @param fd        文件描述符
     * @param namespace 命名空间
     * @param checkSum  是否校验checksum，若校验，失败会抛出FileCorruptedException
     * @return 字节流
     */
    byte[] read(long fd, String namespace, boolean checkSum);

    /**
     * 删除文件
     *
     * @param fd        文件描述符
     * @param namespace 命名空间
     */
    void delete(long fd, String namespace);

}
