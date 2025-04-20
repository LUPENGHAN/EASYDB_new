package DB.log.impl;

import DB.log.interfaces.LogManager;
import DB.log.models.LogRecord;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 日志管理器实现
 */
@Slf4j
public class LogManagerImpl implements LogManager {
    private static final String LOG_FILE = "transaction.log";
    private static final int LOG_BUFFER_SIZE = 8192; // 8KB

    private final File logFile;
    private RandomAccessFile raf;
    private FileChannel channel;
    private final ByteBuffer buffer;
    private final AtomicLong nextLSN;
    private final Map<Long, List<LogRecord>> transactionLogs;
    private LogRecord latestCheckpoint;

    public LogManagerImpl(String logDir) {
        this.logFile = new File(logDir, LOG_FILE);
        this.buffer = ByteBuffer.allocate(LOG_BUFFER_SIZE);
        this.nextLSN = new AtomicLong(1);
        this.transactionLogs = new HashMap<>();
    }

    @Override
    public void init() {
        try {
            if (!logFile.exists()) {
                logFile.createNewFile();
            }
            this.raf = new RandomAccessFile(logFile, "rw");
            this.channel = raf.getChannel();
            recover();
        } catch (IOException e) {
            log.error("Failed to initialize log manager", e);
            throw new RuntimeException("Failed to initialize log manager", e);
        }
    }

    @Override
    public long writeRedoLog(long xid, int pageID, short offset, byte[] newData) {
        LogRecord logRecord = LogRecord.createRedoLog(xid, pageID, offset, newData);
        return writeLog(logRecord);
    }

    @Override
    public long writeUndoLog(long xid, int operationType, byte[] undoData) {
        LogRecord logRecord = LogRecord.createUndoLog(xid, operationType, undoData);
        return writeLog(logRecord);
    }

    private long writeLog(LogRecord logRecord) {
        try {
            long lsn = nextLSN.getAndIncrement();
            byte[] data = logRecord.serialize();
            
            synchronized (this) {
                buffer.clear();
                buffer.putLong(lsn);
                buffer.putInt(data.length);
                buffer.put(data);
                buffer.flip();
                
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            // 更新事务日志缓存
            transactionLogs.computeIfAbsent(logRecord.getXid(), k -> new ArrayList<>())
                    .add(logRecord);

            return lsn;
        } catch (IOException e) {
            log.error("Failed to write log", e);
            throw new RuntimeException("Failed to write log", e);
        }
    }

    @Override
    public LogRecord readLog(long lsn) {
        try {
            synchronized (this) {
                channel.position(0);
                while (channel.position() < channel.size()) {
                    buffer.clear();
                    channel.read(buffer);
                    buffer.flip();
                    
                    long currentLSN = buffer.getLong();
                    if (currentLSN == lsn) {
                        int length = buffer.getInt();
                        byte[] data = new byte[length];
                        buffer.get(data);
                        return LogRecord.deserialize(data);
                    }
                }
            }
            return null;
        } catch (IOException e) {
            log.error("Failed to read log", e);
            throw new RuntimeException("Failed to read log", e);
        }
    }

    @Override
    public List<LogRecord> getTransactionLogs(long xid) {
        return transactionLogs.getOrDefault(xid, new ArrayList<>());
    }

    @Override
    public void createCheckpoint() {
        LogRecord logRecord = new LogRecord();
        logRecord.setLogType(LogRecord.TYPE_CHECKPOINT);
        writeLog(logRecord);
    }

    @Override
    public List<Long> getActiveTransactions() {
        List<Long> activeTransactions = new ArrayList<>();
        for (Map.Entry<Long, List<LogRecord>> entry : transactionLogs.entrySet()) {
            List<LogRecord> logs = entry.getValue();
            if (!logs.isEmpty()) {
                LogRecord lastLog = logs.get(logs.size() - 1);
                if (lastLog.getLogType() != LogRecord.TYPE_UNDO) {
                    activeTransactions.add(entry.getKey());
                }
            }
        }
        return activeTransactions;
    }

    @Override
    public void recover() {
        try {
            channel.position(0);
            while (channel.position() < channel.size()) {
                buffer.clear();
                channel.read(buffer);
                buffer.flip();
                
                long lsn = buffer.getLong();
                int length = buffer.getInt();
                byte[] data = new byte[length];
                buffer.get(data);
                
                LogRecord logRecord = LogRecord.deserialize(data);
                transactionLogs.computeIfAbsent(logRecord.getXid(), k -> new ArrayList<>())
                        .add(logRecord);
                
                if (logRecord.getLogType() == LogRecord.TYPE_CHECKPOINT) {
                    latestCheckpoint = logRecord;
                }
                
                nextLSN.set(lsn + 1);
            }
        } catch (IOException e) {
            log.error("Failed to recover from log", e);
            throw new RuntimeException("Failed to recover from log", e);
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (IOException e) {
            log.error("Failed to close log manager", e);
        }
    }
} 