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
            // 使用通用方法确保channel初始化
            ensureChannelInitialized();
            
            // 从文件中读取日志记录
            readLogsFromFile();
            System.out.println("LogManagerImpl.init(): 完成初始化，从文件读取了日志");
        } catch (Exception e) {
            log.error("Failed to initialize log manager", e);
            throw new RuntimeException("Failed to initialize log manager", e);
        }
    }
    
    /**
     * 从文件读取日志记录，不清空当前事务日志
     */
    private void readLogsFromFile() {
        try {
            // 确保channel已初始化
            ensureChannelInitialized();
            
            // 如果文件是空的，直接返回
            if (channel.size() == 0) {
                System.out.println("LogManagerImpl.readLogsFromFile(): 日志文件为空");
                return;
            }
            
            System.out.println("LogManagerImpl.readLogsFromFile(): 开始读取日志文件，文件大小: " + channel.size());
            channel.position(0);
            
            while (true) {
                // 读取LSN (8字节) 和 长度 (4字节)
                buffer.clear();
                buffer.limit(12); // 只读取LSN和长度字段
                
                int bytesRead = channel.read(buffer);
                if (bytesRead < 12) {
                    // 如果读取不到完整的头部，说明文件已经结束
                    System.out.println("LogManagerImpl.readLogsFromFile(): 读取文件结束，bytesRead=" + bytesRead);
                    break;
                }
                
                buffer.flip();
                long lsn = buffer.getLong();
                int length = buffer.getInt();
                
                System.out.println("LogManagerImpl.readLogsFromFile(): 读取到日志记录，LSN=" + lsn + ", length=" + length);
                
                // 读取日志数据
                byte[] data = new byte[length];
                buffer.clear();
                
                ByteBuffer dataBuffer = ByteBuffer.wrap(data);
                int dataRead = channel.read(dataBuffer);
                if (dataRead < length) {
                    System.out.println("LogManagerImpl.readLogsFromFile(): 警告，日志数据不完整, dataRead=" + dataRead + ", length=" + length);
                    break;
                }
                
                LogRecord logRecord = LogRecord.deserialize(data);
                transactionLogs.computeIfAbsent(logRecord.getXid(), k -> new ArrayList<>())
                        .add(logRecord);
                
                System.out.println("LogManagerImpl.readLogsFromFile(): 添加日志记录，事务ID=" + logRecord.getXid() + 
                                 ", 日志类型=" + logRecord.getLogType());
                
                if (logRecord.getLogType() == LogRecord.TYPE_CHECKPOINT) {
                    latestCheckpoint = logRecord;
                    System.out.println("LogManagerImpl.readLogsFromFile(): 发现检查点日志");
                }
                
                nextLSN.set(lsn + 1);
            }
            
            System.out.println("LogManagerImpl.readLogsFromFile(): 日志文件读取完成，事务数: " + transactionLogs.size());
            for (Map.Entry<Long, List<LogRecord>> entry : transactionLogs.entrySet()) {
                System.out.println("LogManagerImpl.readLogsFromFile(): 事务ID=" + entry.getKey() + 
                                 ", 日志记录数=" + entry.getValue().size());
            }
            
        } catch (IOException e) {
            log.error("Failed to read logs from file", e);
            throw new RuntimeException("Failed to read logs from file", e);
        }
    }

    @Override
    public long writeRedoLog(long xid, int pageID, short offset, byte[] newData) {
        // 确保channel已初始化
        ensureChannelInitialized();
        
        LogRecord logRecord = LogRecord.createRedoLog(xid, pageID, offset, newData);
        return writeLog(logRecord);
    }

    @Override
    public long writeUndoLog(long xid, int operationType, byte[] undoData) {
        // 确保channel已初始化
        ensureChannelInitialized();
        
        LogRecord logRecord = LogRecord.createUndoLog(xid, operationType, undoData);
        return writeLog(logRecord);
    }
    
    /**
     * 确保FileChannel已初始化
     */
    private synchronized void ensureChannelInitialized() {
        try {
            if (channel == null) {
                if (!logFile.exists()) {
                    logFile.createNewFile();
                }
                this.raf = new RandomAccessFile(logFile, "rw");
                this.channel = raf.getChannel();
                System.out.println("LogManagerImpl.ensureChannelInitialized(): 初始化FileChannel");
            }
        } catch (IOException e) {
            log.error("Failed to initialize channel", e);
            throw new RuntimeException("Failed to initialize channel", e);
        }
    }

    private long writeLog(LogRecord logRecord) {
        try {
            long lsn = nextLSN.getAndIncrement();
            byte[] data = logRecord.serialize();
            
            synchronized (this) {
                // 检查channel是否为null，如果是则先初始化
                if (channel == null) {
                    if (!logFile.exists()) {
                        logFile.createNewFile();
                    }
                    this.raf = new RandomAccessFile(logFile, "rw");
                    this.channel = raf.getChannel();
                    System.out.println("LogManagerImpl.writeLog(): 初始化FileChannel");
                }
                
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
                // 确保channel已初始化
                ensureChannelInitialized();
                
                channel.position(0);
                while (channel.position() < channel.size()) {
                    // 每次只读取LSN和长度部分（8 + 4 = 12字节）
                    buffer.clear();
                    buffer.limit(12);
                    int bytesRead = channel.read(buffer);
                    if (bytesRead < 12) {
                        break; // 文件结束或读取不完整
                    }
                    
                    buffer.flip();
                    long currentLSN = buffer.getLong();
                    int length = buffer.getInt();
                    
                    if (currentLSN == lsn) {
                        // 找到目标LSN，读取日志数据
                        byte[] data = new byte[length];
                        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
                        channel.read(dataBuffer);
                        return LogRecord.deserialize(data);
                    } else {
                        // 不是目标LSN，跳过这条日志的数据部分
                        channel.position(channel.position() + length);
                    }
                }
                return null;
            }
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
        
        // 创建检查点时清除所有事务日志，这样在下次获取活跃事务列表时将返回空列表
        transactionLogs.clear();
    }

    @Override
    public List<Long> getActiveTransactions() {
        List<Long> activeTransactions = new ArrayList<>();
        for (Map.Entry<Long, List<LogRecord>> entry : transactionLogs.entrySet()) {
            // 排除xid为0的系统事务（如检查点）
            if (entry.getKey() == 0) {
                continue;
            }
            
            List<LogRecord> logs = entry.getValue();
            if (!logs.isEmpty()) {
                LogRecord lastLog = logs.get(logs.size() - 1);
                // 只有当最后一条日志不是撤销日志时，才认为事务是活跃的
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
            // 确保文件通道已初始化
            ensureChannelInitialized();
            
            System.out.println("LogManagerImpl.recover(): 开始恢复");
            // 清空当前事务日志映射，然后重新读取
            transactionLogs.clear();
            
            // 读取日志文件中的所有日志记录
            readLogsFromFile();
            System.out.println("LogManagerImpl.recover(): 恢复完成");
        } catch (Exception e) {
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