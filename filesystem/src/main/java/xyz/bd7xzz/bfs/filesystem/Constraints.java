package xyz.bd7xzz.bfs.filesystem;

public class Constraints {
    public static final int BLOCK_SIZE = 16 * 1024; //16k一个块
    public static final int EXCEPTION_CODE_INVALID_PARAM = 400;//无效参数code
    public static final int EXCEPTION_CODE_INTERNAL_ERROR = 500;//服务器内部错误
    public static final String FILE_META_BFS = "._bfs";//bfs
    public static final String FILE_META_DELETED = "._deleted";//deleted

    public static final String ENV_KEY_FSYNC = "bfs.fs.writer.sync";//环境变量磁盘同步
    public static final String ENV_KEY_VFS = "bfs.fs.vfs.base";//环境变量vfs
    public static final String ENV_KEY_GZIP = "bfs.fs.writer.gzip";//压缩标记
}
