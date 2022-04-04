package xyz.bd7xzz.bfs.filesystem.impl;

import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import xyz.bd7xzz.bfs.common.Cleanable;
import xyz.bd7xzz.bfs.filesystem.Constraints;
import xyz.bd7xzz.bfs.filesystem.VFSOperator;
import xyz.bd7xzz.bfs.filesystem.exception.FileNotFoundException;
import xyz.bd7xzz.bfs.filesystem.exception.FileSystemException;
import xyz.bd7xzz.bfs.filesystem.struct.FileBlock;
import xyz.bd7xzz.bfs.filesystem.struct.FileDescriptor;
import xyz.bd7xzz.bfs.filesystem.struct.FileMode;
import xyz.bd7xzz.bfs.util.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static xyz.bd7xzz.bfs.filesystem.Constraints.*;

public class FileSystemVFSOperatorImpl implements VFSOperator, Cleanable {
    private static final byte[] EMPTY = new byte[0];
    private static final LongBitmapDataProvider DELETED_BM = new Roaring64NavigableMap();

    @Override
    public void init() {
    }

    @Override
    public void mkVFS(String namespace) {
        if (namespace == null || namespace.trim().length() == 0) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, "invalid namespace");
        }
        try {
            String filePath = buildNamespace(namespace);
            String basePath = getBasePath();
            FileUtil.mkdir(filePath, false); //创建命名空间
            FileUtil.writeFile(basePath + FILE_META_BFS, EMPTY, false, true); //根目录写bfs元信息
            FileUtil.writeFile(filePath + FILE_META_BFS, EMPTY, false, true); //命名空间目录写bfs元信息
            FileUtil.writeFile(basePath + FILE_META_NAME_SPACE, SerializeUtil.stringToByte(namespace), true, true); //根目录下._namespace记录所有命名空间
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "make vfs failed");
        }
    }

    @Override
    public void destroyVFS(String namespace) {
        try {
            String basePath = buildNamespace(namespace);
            FileUtil.writeFile(basePath + FILE_META_BFS, FILE_META_DELETED.getBytes(StandardCharsets.UTF_8), false, true);
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "destroy vfs failed");
        }
    }

    @Override
    public String getPhysicalPath(String namespace, String path) {
        try {
            return buildNamespace(namespace) + path + File.separator;
        } catch (IOException e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "get physical path failed");
        }
    }

    @Override
    public FileDescriptor write(byte[] bytes, FileMode fileMode, String namespace, String path) {
        if (null == bytes || bytes.length == 0) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, "invalid bytes");
        }
        if (null == fileMode) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, "invalid file mode");
        }
        if (null == namespace || namespace.trim().length() == 0) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, "invalid namespace");
        }
        try {
            String filePath = this.getPhysicalPath(namespace, path);
            FileBlock fileBlock;
            List<FileBlock> blockList = new ArrayList<>(bytes.length / BLOCK_SIZE + 1);
            byte[] buffer = new byte[Constraints.BLOCK_SIZE];
            boolean syncFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_FSYNC, false); //根据环境变量配置控制fsync
            boolean gzipFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_GZIP, false);//是否启用gzip压缩
            for (int i = 0; i < bytes.length; i++) { //文件分块写入磁盘
                if (i % Constraints.BLOCK_SIZE == 0) {
                    fileBlock = new FileBlock(IDUtil.generate(), DigestUtil.hex(buffer), buffer.length); //控制块信息
                    FileUtil.writeFile(filePath + fileBlock.getFd(), gzipFlag ? CompressUtil.compress(buffer) : buffer, fileMode == FileMode.APPEND, syncFlag);
                    blockList.add(fileBlock);
                    buffer = new byte[Constraints.BLOCK_SIZE];
                } else {
                    buffer[i] = bytes[i];
                }
            }

            long fd = IDUtil.generate();
            //控制块信息刷盘
            ByteBuffer byteBuffer = ByteBuffer.allocate(blockList.size() * FileBlock.size());
            for (FileBlock block : blockList) {
                byteBuffer.put(block.toByte());
            }
            FileUtil.writeFile(filePath + "._" + fd, CompressUtil.compress(byteBuffer.array()), false, true);//控制块强制刷盘
            return new FileDescriptor(fd, DigestUtil.hex(bytes), blockList);
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "write file failed!");
        }
    }

    @Override
    public FileDescriptor write(byte[] bytes, String namespace, String path) {
        return write(bytes, FileMode.CREATE, namespace, path);
    }

    @Override
    public byte[] read(FileDescriptor fd, String namespace) {
        if (null == fd) {
            throw new FileSystemException(Constraints.EXCEPTION_CODE_INVALID_PARAM, "invalid file descriptor");
        }
        if (DELETED_BM.contains(fd.getFd())) {
            throw new FileNotFoundException(fd.getFd());
        }
        try {
            //再过一下._deleted文件
            iteratorDeletedFd(namespace, deletedFd -> {
                DELETED_BM.addLong(deletedFd);//加载到bitmap中
                if (deletedFd == fd.getFd()) {
                    throw new FileNotFoundException(fd.getFd());
                }
            });

            boolean gzipFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_GZIP, false);//是否启用gzip压缩
            return null;
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "read file failed!");
        }
    }

    @Override
    public void delete(FileDescriptor fd, String namespace) {
        if (null == fd) {
            throw new FileSystemException(Constraints.EXCEPTION_CODE_INVALID_PARAM, "invalid file descriptor");
        }
        try {
            FileUtil.writeFile(getPhysicalPath(namespace, FILE_META_DELETED), SerializeUtil.longToByte(fd.getFd()), true, true);
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "delete file failed!");
        }
        try {
            DELETED_BM.addLong(fd.getFd());
        } catch (Exception e) {

        }
    }

    @Override
    public void cleanup() {
        while (DELETED_BM.getLongIterator().hasNext()) {
            long fd = DELETED_BM.getLongIterator().next();

            DELETED_BM.removeLong(fd);
        }
    }

    /**
     * 获取虚拟文件系统的根路径
     *
     * @return 绝对路径
     */
    private String getBasePath() {
        String basePath = EnvironmentVariableUtil.getString(ENV_KEY_VFS);
        if (null == basePath || basePath.trim().length() == 0) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, "invalid vfs base path");
        }
        return basePath + File.separator;
    }

    /**
     * 虚拟文件系统是否有效
     *
     * @param physicalPath 绝对路径
     * @throws IOException 无效抛出异常
     */
    private void isValidVFS(String physicalPath) throws IOException {
        String bfsFile = physicalPath + FILE_META_BFS;
        if (!FileUtil.exists(bfsFile)) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, physicalPath + " invalid");
        }
        byte[] bytes = FileUtil.readFile(bfsFile);
        if (!new String(bytes, StandardCharsets.UTF_8).trim().contains("deleted")) {
            throw new FileSystemException(EXCEPTION_CODE_INVALID_PARAM, physicalPath + " invalid");
        }
    }

    /**
     * 构建命名空间绝对路径
     *
     * @param namespace 命名空间
     * @return 命名空间绝对路径
     * @throws IOException
     */
    private String buildNamespace(String namespace) throws IOException {
        String basePath = getBasePath();
        isValidVFS(basePath);
        return basePath + namespace + File.separator;
    }


    /**
     * 迭代._delete文件，执行业务逻辑
     *
     * @param namespace 命名空间
     * @param consumer  执行业务逻辑
     * @throws IOException
     */
    private void iteratorDeletedFd(String namespace, Consumer<Long> consumer) throws IOException {
        byte[] bytes = FileUtil.readFile(getPhysicalPath(namespace, FILE_META_DELETED));
        if (bytes.length == 0) {
            return;
        }
        for (int i = 0; i < bytes.length; i += Long.SIZE) {
            byte[] tmp = new byte[Long.SIZE];
            System.arraycopy(bytes, i, tmp, 0, Long.SIZE);
            long deletedFd = SerializeUtil.byteToLong(tmp);
            consumer.accept(deletedFd);
        }
    }
}
