package com.tsg.lq.store;

import com.tsg.lq.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 消息存储管理
 *
 * Created by tsguang on 2019/8/16.
 */
public class CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private static final int CACHE_SIZE = 100;  //缓存消息的带下，提高读的效率
    private String dataDir; //存放队列文件的目录
    private String topic;   //对应的队列名称
    private String fileNamePrefix;
    private ArrayList<MappedFile> fileList = new ArrayList();   //TODO tsg : linkedList?? 一个队列对应多个文件，一个文件对应一个MappedFile
    private MappedFile readFile;   //当前读取的文件
    private MappedFile writeFile;  //当前写入的文件
    private Lock lock = new ReentrantLock();
    private Condition notEmpty;
    private volatile boolean closed;


    public CommitLog() {
    }

    public CommitLog(String dataDir, String topic)throws IOException {
        this.notEmpty = this.lock.newCondition();
        this.closed = false;
        this.dataDir = dataDir;
        this.topic = topic;
        this.fileNamePrefix = Constants.DEFAULT_QUEUE_FILE_PREFIX + this.topic + Constants.DEFAULT_QUEUE_FILE_SUFFIX;
        try {
            init();
        }catch (IOException e){
            logger.error("创建topic存储信息异常，topic = {}",topic,e);
            close();
            throw e;
        }
    }

//    public String getFileNamePrefix() {
//        return Constants.DEFAULT_QUEUE_FILE_PREFIX + this.topic + Constants.DEFAULT_QUEUE_FILE_SUFFIX;
//    }

    public String getFileName(int fileNo) {
        return this.fileNamePrefix + fileNo;
    }

    private void init() throws IOException {
        String fileNamePrefix = this.fileNamePrefix;
        ArrayList<Integer> fileNoList = new ArrayList();
        File[] files = new File(this.dataDir).listFiles();
        for (int i = 0; i < files.length; ++i) {
            File file = files[i];
            String fileName = file.getName();
            int index = fileName.indexOf(fileNamePrefix);
            if (index == 0) {
                String fileNoString = fileName.substring(index + fileNamePrefix.length());
                fileNoList.add(Integer.parseInt(fileNoString));
            }
        }

        Collections.sort(fileNoList);
        Iterator<Integer> iterator = fileNoList.iterator();

        while (iterator.hasNext()) {
            int fileNo = iterator.next();
            MappedFile mappedFile = this.loadFile(fileNo);
            if (mappedFile.isOpened()) {
                this.fileList.add(mappedFile);
            } else {
                mappedFile.drop();
                logger.info("data file dropped, filename=" + mappedFile.getFileName());
            }
        }

        if (this.fileList.size() == 0) {
            MappedFile mappedFile = this.createFile(1);
            this.fileList.add(mappedFile);
        }

        this.readFile = this.fileList.get(0);
        this.writeFile = this.fileList.get(this.fileList.size() - 1);
        logger.info("CommitLog {} started", this.topic);
    }

    public boolean isClosed() {
        return this.closed;
    }

    public void close() {
        this.lock.lock();
        try {
            for (MappedFile mf : this.fileList) {
                mf.close();
            }
            this.closed = true;
        } finally {
            this.lock.unlock();
        }
        logger.info("CommitLog {} closed", this.topic);
    }

    public MappedFile createFile(int fileNo) throws IOException {
        String fileName = this.fileNamePrefix + fileNo;
        MappedFile mappedFile = MappedFile.createNew(this.dataDir,fileName);
        return mappedFile;
    }

    public MappedFile loadFile(int fileNo) throws IOException {
        String fileName = this.fileNamePrefix + fileNo;
        MappedFile mappedFile = MappedFile.load(this.dataDir,fileName);
        return mappedFile;
    }

    public boolean empty() {
        this.lock.lock();
        try {
            for (MappedFile mf : this.fileList) {
                if (!mf.checkEmpty()) {
                    return false;
                }
            }
            return true;
        } finally {
            this.lock.unlock();
        }
    }

    public int size() {
        int total = 0;
        this.lock.lock();
        try {
            for (MappedFile mf : this.fileList) {
                total += mf.size();
            }
            return total;
        } finally {
            this.lock.unlock();
        }
    }

    public int cacheSize() {
        int total = 0;
        this.lock.lock();
        try {
            for (MappedFile mf : this.fileList) {
                total += mf.cacheSize();
            }
            return total;
        } finally {
            this.lock.unlock();
        }
    }

    public void purge() throws IOException {
        if (!this.closed) {
            this.lock.lock();
            try {
                if (!this.closed) {
                    while (this.readFile.readCompleted()) {
                        int readFileNo = analyseFileNo(this.readFile.getFileName());
                        MappedFile mf = getReadFile(readFileNo + 1);
                        if (mf == null) {   //恰好读完
                            int writeFileNo = analyseFileNo(this.writeFile.getFileName());
                            mf = createFile(writeFileNo + 1);
                            this.fileList.add(mf);
                            this.writeFile = mf;
                        }
                        this.readFile = mf;
                    }
                    MappedFile f;
                    /*for(; this.readFile.readCompleted(); this.readFile = f) {
                        f = this.getReadFile(this.readFile.fileno + 1);
                        if (f == null) {
                            f = this.createFile(this.writeFile.fileno + 1);
                            this.fileList.add(f);
                            this.writeFile = f;
                        }
                    }*/

                    ArrayList<MappedFile> deleteList = new ArrayList();
                    Iterator<MappedFile> iterator = this.fileList.iterator();

                    while (true) {
                        MappedFile f1;
                        if (iterator.hasNext()) {
                            f1 = iterator.next();
                            if (analyseFileNo(f1.getFileName()) < analyseFileNo(this.readFile.getFileName())) {
                                if (f1.checkCompeted()) {
                                    deleteList.add(f1);
                                }
                                continue;
                            }
                        }

                        iterator = deleteList.iterator();

                        while (iterator.hasNext()) {
                            f = iterator.next();
                            this.fileList.remove(f);
                            f.close();
                            f.drop();
                            logger.info("data file closed, filename=" + f.getFileName());
                        }

                        return;
                    }
                }
            } finally {
                this.lock.unlock();
            }
        }
    }

    private int analyseFileNo(String fileName){
        String filePrifix = this.fileNamePrefix;
        int index = fileName.indexOf(filePrifix);
        String fileNoString = fileName.substring(index + filePrifix.length());
        return Integer.parseInt(fileNoString);
    }

    public MappedFile getReadFile(int fileNo) {
        String fn = getFileName(fileNo);
        if (fn.equals(this.readFile.getFileName())){
            return this.readFile;
        } else {
            for (MappedFile mf : this.fileList) {
                if (fn.equals(mf.getFileName())) {
                    return mf;
                }
            }
            return null;
        }
    }

    public void put(String message) throws IOException {
        this.putAndReturnIdx(message);
    }

    public long putAndReturnIdx(String message) throws IOException {
        if (message != null && !message.equals("")) {
            byte[] bs;
            try {
                bs = message.getBytes("UTF-8");
            } catch (Exception e) {
                return -1L;
            }

            return this.putAndReturnIdx(bs);
        } else {
            return -1L;
        }
    }

    public void put(byte[] bs) throws IOException {
        this.putAndReturnIdx(bs);
    }

    public long putAndReturnIdx(byte[] bs) throws IOException {
        if (bs != null && bs.length != 0) {
            long ret = 0L;
            this.lock.lock();

            try {
                int idx = this.writeFile.write(bs);
                if (idx == -1) {
                    MappedFile f = this.createFile(analyseFileNo(this.writeFile.getFileName()) + 1);
                    this.fileList.add(f);
                    this.writeFile = f;
                    idx = this.writeFile.write(bs);
                }

                ret = ((long) analyseFileNo(this.writeFile.getFileName()) << 32) + (long) idx;
                this.notEmpty.signal();
            } finally {
                this.lock.unlock();
            }

            return ret;
        } else {
            return -1L;
        }
    }

    public long get() throws IOException, InterruptedException {
        return this.get(-1L);
    }

    public long get(long wait) throws IOException, InterruptedException {
        if (wait < -1L) {
            wait = -1L;
        }

        this.lock.lock();

        try {
            while (true) {
                int idx = this.readFile.get();
                if (idx != MappedFile.FULL_EXCEPTION) {
                    long res;
                    if (idx == MappedFile.EMPTY_EXCEPTION) {
                        if (wait == 0L) {
                            res = -1L;
                            return res;
                        }

                        if (wait == -1L) {
                            this.notEmpty.await();
                            continue;
                        }

                        if (wait > 0L) {
                            boolean ok = this.notEmpty.await(wait, TimeUnit.MILLISECONDS);
                            if (ok) {
                                continue;
                            }
                            return -1L;
                        }
                    }

                    res = ((long) analyseFileNo(this.readFile.getFileName()) << 32) + (long) idx;
                    return res;
                } else {
                    MappedFile f = this.getReadFile(analyseFileNo(this.readFile.getFileName()) + 1);
                    if (f == null) {
                        f = this.createFile(analyseFileNo(this.writeFile.getFileName()) + 1);
                        this.fileList.add(f);
                        this.writeFile = f;
                    }

                    this.readFile = f;
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

    public byte[] getBytes(long idx) {
        this.lock.lock();

        byte[] result;
        try {
            int fileNo = (int) (idx >> 32);
            int fileIdx = (int) (idx & -1L);
            MappedFile f = this.getReadFile(fileNo);//获取到是哪个队列文件
            if (f == null) {
                return null;
            }
            result = f.item(fileIdx);//读取文件内容
        } catch (IOException var10) {
            return null;
        } finally {
            this.lock.unlock();
        }
        return result;
    }

    public String getString(long idx) {
        byte[] bs = this.getBytes(idx);
        if (bs == null) {
            return null;
        } else {
            try {
                return new String(bs, "UTF-8");
            } catch (Exception var5) {
                return null;
            }
        }
    }

    public void commit(long idx) {
        this.lock.lock();

        try {
            int fileNo = (int) (idx >> 32);
            int fileIdx = (int) (idx & -1L);
            MappedFile f = this.getReadFile(fileNo);
            if (f != null) {
                f.commit(fileIdx);
                return;
            }
        } finally {
            this.lock.unlock();
        }

    }

    public void rollback(long idx) {
        this.lock.lock();

        try {
            int fileNo = (int) (idx >> 32);
            int fileIdx = (int) (idx & -1L);
            MappedFile f = this.getReadFile(fileNo);
            if (f == null) {
                return;
            }

            f.rollback(fileIdx);
            if (this.readFile != f) {
                this.readFile = f;
            }
        } finally {
            this.lock.unlock();
        }

    }

    public String getDataDir() {
        return this.dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public static boolean checkCompetedOnOpen(String filename) {
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(filename, "rw");
            byte[] bs = new byte[MappedFile.DIR_SIZE];

            try {
                raf.read(bs);
            } catch (Exception e) {
                return false;
            }

            for (int i = 0; i < MappedFile.DIR_ENTRY_COUNT; i++) {
                byte status = bs[i * MappedFile.DIR_ENTRY_SIZE];
                if (status == MappedFile.STATUS_BUSY || status == MappedFile.STATUS_AVAILABLE) {
                    return false;
                }
            }

            return true;
        } catch (Exception e1) {
            return false;
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                    raf = null;
                } catch (Exception var17) {
                    logger.error("cannot close raf (" + filename + ")", var17);
                }
            }

        }
    }

}
