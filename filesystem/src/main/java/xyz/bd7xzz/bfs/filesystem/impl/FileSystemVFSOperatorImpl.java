package xyz.bd7xzz.bfs.filesystem.impl;

import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import xyz.bd7xzz.bfs.common.Cleanable;
import xyz.bd7xzz.bfs.filesystem.Constraints;
import xyz.bd7xzz.bfs.filesystem.VFSOperator;
import xyz.bd7xzz.bfs.filesystem.exception.FileCorruptedException;
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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static xyz.bd7xzz.bfs.filesystem.Constraints.*;

/**
 * namespace隔离不同命名空间，磁盘中带有._bfs代表bfs的文件系统，._namespace记录了所有命名空间
 * 若._bfs内容为._delete，则命名空间作废
 * 命名空间下包含._deleted文件，记录了被删除的文件描述符，真实删除文件由gc调用cleanup方法执行物理删除
 * 文件描述符结构会记录在每个用户创建的路径中，以._开头+描述符编号的文件中，同时该文件会软连接到命名空间根目录，方便查询
 * 若文件无法关联到文件描述符结构文件，称为孤儿文件，由gc调用cleanup方法执行物理删除
 * 孤儿文件的产生可能因为写文件块后，系统异常退出，还没有写文件描述符结构导致，此时认为文件彻底写入失败，客户端可进行重试
 */
public class FileSystemVFSOperatorImpl implements VFSOperator, Cleanable {
    private static final byte[] EMPTY = new byte[0];
    private static final LongBitmapDataProvider EMPTY_BM = Roaring64NavigableMap.bitmapOf();
    private static final Map<String, LongBitmapDataProvider> DELETED_BM = new ConcurrentHashMap<>();
    private static final AtomicBoolean INIT_FLAT = new AtomicBoolean(false);

    @Override
    public void init() {
        try {
            if (INIT_FLAT.compareAndSet(false, true)) {
                byte[] data = FileUtil.readFile(getBasePath() + FILE_META_NAME_SPACE); //初始化每个命名空间的bitmap
                String dataStr = SerializeUtil.byteToString(data).trim();
                if (dataStr.length() > 0) {
                    String[] namespaces = dataStr.split("\n");
                    for (String namespace : namespaces) {
                        DELETED_BM.put(namespace, new Roaring64NavigableMap());
                        iteratorDeletedFd(namespace, fd -> DELETED_BM.get(namespace).addLong(fd));
                    }
                }
            }
        } catch (Exception e) {
            //TODO 记录日志
            System.exit(-1);
        }
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
            FileUtil.writeFile(basePath + FILE_META_NAME_SPACE, SerializeUtil.stringToByte(namespace + "\n"), true, true); //根目录下._namespace记录所有命名空间
            DELETED_BM.put(namespace, new Roaring64NavigableMap());//初始化bitmap
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
    public long write(byte[] bytes, FileMode fileMode, String namespace, String path, String fileName) {
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
            FileUtil.mkdir(filePath, false);
            FileBlock fileBlock;
            List<FileBlock> blockList = new ArrayList<>(bytes.length / BLOCK_SIZE + 1);
            byte[] buffer = new byte[Constraints.BLOCK_SIZE];
            boolean syncFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_FSYNC, false); //根据环境变量配置控制fsync
            boolean gzipFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_GZIP, false);//是否启用gzip压缩
            for (int i = 0; i < bytes.length; i++) { //文件分块写入磁盘
                if (i % Constraints.BLOCK_SIZE == 0) {
                    fileBlock = new FileBlock(IDUtil.generate(), DigestUtil.hex(buffer)); //控制块信息
                    FileUtil.writeFile(filePath + fileBlock.getFd(), gzipFlag ? CompressUtil.compress(buffer) : buffer, fileMode == FileMode.APPEND, syncFlag);
                    blockList.add(fileBlock);
                    buffer = new byte[Constraints.BLOCK_SIZE];
                } else {
                    buffer[i] = bytes[i];
                }
            }
            long fd = IDUtil.generate();
            FileDescriptor fileDescriptor = new FileDescriptor();
            fileDescriptor.setFd(fd);
            fileDescriptor.setHex(DigestUtil.hex(bytes));
            fileDescriptor.setSize(bytes.length);
            fileDescriptor.setFileBlocks(blockList);
            fileDescriptor.setPhysicalPath(filePath);
            fileDescriptor.setFileName(fileName);
            fileDescriptor.setCreateTime(System.currentTimeMillis());
            String fdPath = filePath + "._" + fd;
            FileUtil.writeFile(fdPath, CompressUtil.compress(SerializeUtil.objectToByte(fileDescriptor)), false, true); //写文件描述符结构
            Files.createSymbolicLink(FileSystems.getDefault().getPath(buildNamespace(namespace) + "._" + fd),
                    FileSystems.getDefault().getPath(fdPath)); //记录软链接到namespace下，方便查找
            return fd;
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "write file failed!");
        }
    }

    @Override
    public long write(byte[] bytes, String namespace, String path, String fileName) {
        return write(bytes, FileMode.CREATE, namespace, path, fileName);
    }

    @Override
    public byte[] read(long fd, String namespace, boolean checkSum) {
        if (fd <= 0) {
            throw new FileSystemException(Constraints.EXCEPTION_CODE_INVALID_PARAM, "invalid file descriptor");
        }
        LongBitmapDataProvider deletedBM = DELETED_BM.getOrDefault(namespace, EMPTY_BM);
        if (deletedBM.contains(fd)) {
            throw new FileNotFoundException(fd);
        }
        try {
            //再过一下._deleted文件
            iteratorDeletedFd(namespace, deletedFd -> {
                deletedBM.addLong(deletedFd);//加载到bitmap中
                if (deletedFd == fd) {
                    throw new FileNotFoundException(fd);
                }
            });

            //从文件描述符信息中取控制块，根据控制块逐一读取文件流
            FileDescriptor fileDescriptor = getFileDescriptor(fd, namespace);
            if (null == fileDescriptor) {
                throw new FileNotFoundException(fd);
            }
            String filePath = fileDescriptor.getPhysicalPath();
            boolean gzipFlag = EnvironmentVariableUtil.getBoolean(ENV_KEY_GZIP, false);//是否启用gzip压缩
            ByteBuffer byteBuffer = ByteBuffer.allocate(Long.valueOf(fileDescriptor.getSize()).intValue());
            for (FileBlock fileBlock : fileDescriptor.getFileBlocks()) {
                byte[] blockBytes = FileUtil.readFile(filePath + fileBlock.getFd());
                blockBytes = gzipFlag ? CompressUtil.unCompress(blockBytes) : blockBytes;
                if (checkSum) {
                    byte[] hex = DigestUtil.hex(blockBytes);
                    if (!DigestUtil.hexEquals(hex, fileBlock.getHex())) { //校验每一个块是否正确
                        throw new FileCorruptedException(fd, fileBlock.getFd());
                    }
                }
                byteBuffer.put(blockBytes);
            }
            byte[] data = byteBuffer.array();
            if (checkSum) {
                if (!DigestUtil.hexEquals(fileDescriptor.getHex(), DigestUtil.hex(data))) { //校验最终文件是否正确
                    throw new FileCorruptedException(fd, fd);
                }
            }
            return data;
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "read file failed!");
        }
    }

    @Override
    public void delete(long fd, String namespace) {
        if (fd <= 0) {
            throw new FileSystemException(Constraints.EXCEPTION_CODE_INVALID_PARAM, "invalid file descriptor");
        }
        try {
            FileUtil.writeFile(getPhysicalPath(namespace, FILE_META_DELETED), SerializeUtil.longToByte(fd), true, true);
        } catch (Exception e) {
            throw new FileSystemException(EXCEPTION_CODE_INTERNAL_ERROR, "delete file failed!");
        }
        try {
            DELETED_BM.get(namespace).addLong(fd);
        } catch (Exception e) {
            //TODO 记录日志
        }
    }

    @Override
    public void cleanup() {
        //TODO
    }

    /**
     * 获取文件描述符结构
     *
     * @param fd
     * @return
     */
    private FileDescriptor getFileDescriptor(long fd, String namespace) throws IOException {
        String path = buildNamespace(namespace);
        byte[] bytes = CompressUtil.unCompress(FileUtil.readFile(path + "._" + fd));
        return SerializeUtil.byteToObject(FileDescriptor.class, bytes);
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
        for (int i = 0; i < bytes.length; i += 8) {
            byte[] tmp = new byte[8];
            System.arraycopy(bytes, i, tmp, 0, 8);
            long deletedFd = SerializeUtil.byteToLong(tmp);
            consumer.accept(deletedFd);
        }
    }

}
