package com.tsg.lq.store;

import com.tsg.lq.util.UnMapTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 文件映射
 * <p>
 * Created by tsguang on 2019/8/16.
 */
public class MappedFile {

    private static final Logger logger = LoggerFactory.getLogger(MappedFile.class);

    private static final byte STATUS_EMPTY = 0; //空，默认状态
    private static final byte STATUS_AVAILABLE = 1; //写入，待消费状态
    private static final byte STATUS_BUSY = 2;  //正在被消费中
    private static final byte STATUS_COMMIT = 3;  //消费成功
    private static final int FULL_EXCEPTION = -1;   //队列文件写满
    private static final int EMPTY_EXCEPTION = -2;  //队列文件空，没有要消费的消息

    private static final int DIR_ENTRY_SIZE = 5;    //记录一个消息的索引的大小，1 byte 记录状态， 1 int(4 byte) 记录消息在文件的地址
    private static final int DIR_ENTRY_COUNT = 500000;  //一个文件最多存放的消息数
    private static final int DIR_SIZE = DIR_ENTRY_COUNT * DIR_ENTRY_SIZE;    //一个队列文件存放索引的大小

    private String dataDir; //文件编号，单个文件大小上限
    private String filename;    //具体文件名
    private RandomAccessFile raf;
    private FileChannel fc;
    private MappedByteBuffer mbb;
    private int readPosition = -1;  //当前文件读指针位置     //TODO tsg : AtomicInteger ?? 每个file文件只存储一个topic消息，单线程写
    private int writePosition = -1; //当前文件写指针位置
    private volatile int realLimit = DIR_ENTRY_COUNT;   //实际写入消息的大小

    private int cacheSize;
    private ConcurrentHashMap<Integer, byte[]> msgMap = new ConcurrentHashMap();

    private MappedFile(String dataDir, String filename) throws IOException{
        try {
            this.dataDir = dataDir;
            this.filename = filename;
            this.raf = new RandomAccessFile(dataDir + File.separator + this.filename, "rw");
            this.fc = this.raf.getChannel();
            this.mbb = this.fc.map(FileChannel.MapMode.READ_WRITE, 0L, DIR_SIZE);
        } catch (IOException e) {
            logger.error("cannot create file(" + dataDir + File.separator + this.filename + ") e=" + e.getMessage());
            throw e;
        }
    }

    public static MappedFile createNew(String dataDir, String filename)throws IOException{
        MappedFile mappedFile = new MappedFile(dataDir,filename);

        for (int i = 0; i < DIR_ENTRY_COUNT; ++i) {
            mappedFile.mbb.position(i * DIR_ENTRY_SIZE);
            mappedFile.mbb.put(STATUS_EMPTY);
        }

        mappedFile.readPosition = 0;
        mappedFile.writePosition = 0;
        return mappedFile;
    }

    public int cacheSize() {
        return this.msgMap.size();
    }

    public int hashCode() {
        return this.filename.hashCode();
    }

    public boolean equals(Object b) {
        return !(b instanceof MappedFile) ? false : this.filename.equals(((MappedFile) b).filename);
    }

    public boolean writeCompleted() {
        return writePosition >= realLimit;
    }

    public boolean readCompleted() {
        return this.readPosition >= realLimit;
    }

    //TODO tsg : 加锁？
    public int get() {
        if (this.readCompleted()) { //当前文件已经写满
            return FULL_EXCEPTION;
        } else {
            while (true) {
                int idx = this.readPosition;
                if (this.readCompleted()) {
                    return FULL_EXCEPTION;
                }

                this.mbb.position(idx * DIR_ENTRY_SIZE);
                byte status = this.mbb.get();
                if (status == STATUS_EMPTY) {   //没有新消息
                    return EMPTY_EXCEPTION;
                }

                if (status == STATUS_AVAILABLE) {   //读取到可消费的消息
                    this.mbb.position(idx * DIR_ENTRY_SIZE);
                    this.mbb.put(STATUS_BUSY);
                    this.readPosition++;
                    return idx;     //返回消息索引
                }

                if (status == STATUS_BUSY) {
                    ++this.readPosition;
                } else if (status == STATUS_COMMIT) {
                    ++this.readPosition;
                }
            }
        }
    }

    /**
     * 返回队列里面对应的消息
     *
     * @param idx
     * @return
     * @throws IOException
     */
    byte[] item(int idx) throws IOException {
        if (idx >= 0 && idx < DIR_ENTRY_COUNT) {
            byte[] bs;
            //定位
            this.mbb.position(idx * DIR_ENTRY_SIZE);
            this.mbb.get();
            int offset = this.mbb.getInt();

            try {//从文件读取
                this.raf.seek((long) offset);
                int length = this.raf.readShort() & '\uffff';
                bs = new byte[length];      //TODO tsg : byte[] --> ByteBuffer ?
                this.raf.read(bs);
                return bs;
            } catch (IOException e) {
                logger.error("item() exception in (" + this.filename + ") e=" + e.getMessage());
                throw e;
            }
        } else {
            return null;
        }
    }

    /**
     * 消息消费确认
     *
     * @param idx 消息在文件的索引
     */
    public void commit(int idx) {
        if (idx >= 0 && idx < DIR_ENTRY_COUNT) {
            this.mbb.position(idx * DIR_ENTRY_SIZE);
            this.mbb.put(STATUS_COMMIT);
        }
    }

    /**
     * 消息回滚
     *
     * @param idx 消息在文件的索引
     */
    public void rollback(int idx) {
        if (idx >= 0 && idx < DIR_ENTRY_COUNT) {
            this.mbb.position(idx * DIR_ENTRY_SIZE);
            this.mbb.put(STATUS_AVAILABLE);
            this.readPosition = idx;
        }
    }

    /**
     * 消息写入
     *
     * @param bs 消息数组
     * @return
     * @throws IOException
     */
    public int write(byte[] bs) throws IOException {
        if (this.writeCompleted()) {
            return -1;
        } else {
            int position;
            try {
                position = (int) this.raf.length();
                this.raf.seek(position);
                this.raf.writeShort(bs.length);
                this.raf.write(bs);
            } catch (IOException e) {
                logger.error("write() exception in (" + this.filename + ") e=" + e.getMessage());
                throw e;
            }

            this.mbb.position(this.writePosition * DIR_ENTRY_SIZE);
            this.mbb.put(STATUS_AVAILABLE);
            this.mbb.putInt(position);

            int lastWritePosition = this.writePosition++;
            if (raf.length() > Integer.MAX_VALUE) {
                realLimit = writePosition;
            }
            return lastWritePosition;
        }
    }

    /**
     * 卸载
     */
    public void drop() {
        (new File(this.dataDir + File.separator + this.filename)).delete();
    }

    /**
     * 检查消息是否消费完，管理（重启恢复时）使用
     *
     * @return
     */
    public boolean checkCompeted() {
        for (int i = 0; i < DIR_ENTRY_COUNT; ++i) {
            this.mbb.position(i * DIR_ENTRY_SIZE);
            byte status = this.mbb.get();
            if (status != STATUS_COMMIT) {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查是否有待消费消息，管理（重启恢复时）使用
     *
     * @return
     */
    public boolean checkEmpty() {
        for (int i = 0; i < DIR_ENTRY_COUNT; ++i) {
            this.mbb.position(i * DIR_ENTRY_SIZE);
            byte status = this.mbb.get();
            if (status != STATUS_COMMIT && status != STATUS_EMPTY) {
                return false;
            }
        }
        return true;
    }

    /**
     * 剩余未消费消息数量，管理（重启恢复时）使用
     *
     * @return
     */
    public int size() {
        int total = 0;
        for (int i = 0; i < DIR_ENTRY_COUNT; ++i) {
            this.mbb.position(i * DIR_ENTRY_SIZE);
            byte status = this.mbb.get();
            if (status != STATUS_COMMIT && status != STATUS_EMPTY) {
                total++;
            }
        }
        return total;
    }

    /**
     * 是否开启，能有效读写
     *
     * @return
     */
    public boolean isOpened() {
        return this.raf != null;
    }


    //TODO tsg : 待定  create??
    public static MappedFile load(String dataDir,String filename) throws IOException {
        MappedFile mappedFile = new MappedFile(dataDir,filename);

        int i;
        byte status;
        for (i = 0; i < DIR_ENTRY_COUNT; i++) {
            mappedFile.mbb.position(i * DIR_ENTRY_SIZE);
            status = mappedFile.mbb.get();
            if (status == STATUS_BUSY) {
                mappedFile.mbb.position(i * DIR_ENTRY_SIZE);
                mappedFile.mbb.put(STATUS_AVAILABLE);
            }
        }

        for (i = 0; i < DIR_ENTRY_COUNT; i++) {
            mappedFile.mbb.position(i * DIR_ENTRY_SIZE);
            status = mappedFile.mbb.get();
            if (status == STATUS_EMPTY) {
                break;
            }
        }

        mappedFile.writePosition = i;

        for (i = 0; i < DIR_ENTRY_COUNT; ++i) {
            mappedFile.mbb.position(i * DIR_ENTRY_SIZE);
            status = mappedFile.mbb.get();
            if (status == STATUS_EMPTY || status == STATUS_AVAILABLE) {
                break;
            }
        }

        mappedFile.readPosition = i;
        return mappedFile;
    }

    public void close() {
        if (this.mbb != null) {
            try {
                UnMapTool.unmap(this.mbb);
            } catch (Exception e) {

            }
        }

        if (this.fc != null) {
            try {
                this.fc.close();
                this.fc = null;
            } catch (Exception e1) {
                logger.error("cannot close fc (" + this.filename + ")", e1);
            }
        }

        if (this.raf != null) {
            try {
                this.raf.close();
                this.raf = null;
            } catch (Exception e2) {
                logger.error("cannot close raf (" + this.filename + ")", e2);
            }
        }

    }

}
