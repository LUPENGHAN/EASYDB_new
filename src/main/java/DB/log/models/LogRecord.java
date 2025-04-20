package DB.log.models;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@NoArgsConstructor
public class LogRecord {
    public static final byte TYPE_REDO = 0;
    public static final byte TYPE_UNDO = 1;
    public static final byte TYPE_CHECKPOINT = 2;
    public static final byte TYPE_COMPENSATION = 3;
    public static final byte TYPE_END_CHECKPOINT = 4;
    public static final byte TYPE_BEGIN_CHECKPOINT = 5;

    public static final int UNDO_INSERT = 0;
    public static final int UNDO_DELETE = 1;
    public static final int UNDO_UPDATE = 2;

    private byte logType;
    private long xid;
    private long logRecordLength;

    private int pageID;
    private short offset;
    private byte[] oldData = new byte[0];
    private byte[] newData = new byte[0];

    private int operationType;
    private byte[] undoData = new byte[0];

    public static LogRecord createRedoLog(long xid, int pageID, short offset, byte[] newData) {
        LogRecord log = new LogRecord();
        log.logType = TYPE_REDO;
        log.xid = xid;
        log.pageID = pageID;
        log.offset = offset;
        log.newData = newData != null ? newData : new byte[0];
        log.logRecordLength = 8 + 4 + 2 + 4 + log.newData.length;
        return log;
    }

    public static LogRecord createUndoLog(long xid, int operationType, byte[] undoData) {
        LogRecord log = new LogRecord();
        log.logType = TYPE_UNDO;
        log.xid = xid;
        log.operationType = operationType;
        log.undoData = undoData != null ? undoData : new byte[0];
        log.logRecordLength = 8 + 4 + 4 + log.undoData.length;
        return log;
    }

    public byte[] serialize() {
        byte[] body = switch (logType) {
            case TYPE_REDO -> serializeRedoLog();
            case TYPE_UNDO -> serializeUndoLog();
            default -> new byte[0];
        };

        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + body.length);
        buffer.put(logType);
        buffer.putInt(body.length);
        buffer.put(body);
        return buffer.array();
    }

    private byte[] serializeRedoLog() {
        ByteBuffer buffer = ByteBuffer.allocate((int) logRecordLength);
        buffer.putLong(xid);
        buffer.putInt(pageID);
        buffer.putShort(offset);
        buffer.putInt(newData.length);
        buffer.put(newData);
        return buffer.array();
    }

    private byte[] serializeUndoLog() {
        ByteBuffer buffer = ByteBuffer.allocate((int) logRecordLength);
        buffer.putLong(xid);
        buffer.putInt(operationType);
        buffer.putInt(undoData.length);
        buffer.put(undoData);
        return buffer.array();
    }

    public static LogRecord deserialize(byte[] entry) {
        ByteBuffer buffer = ByteBuffer.wrap(entry);
        byte type = buffer.get();
        int len = buffer.getInt();
        byte[] body = new byte[len];
        buffer.get(body);

        return switch (type) {
            case TYPE_REDO -> deserializeRedoLog(body);
            case TYPE_UNDO -> deserializeUndoLog(body);
            default -> null;
        };
    }

    private static LogRecord deserializeRedoLog(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        LogRecord log = new LogRecord();
        log.logType = TYPE_REDO;
        log.xid = buffer.getLong();
        log.pageID = buffer.getInt();
        log.offset = buffer.getShort();
        int newDataLen = buffer.getInt();
        log.newData = new byte[newDataLen];
        buffer.get(log.newData);
        log.logRecordLength = 8 + 4 + 2 + 4 + newDataLen;
        return log;
    }

    private static LogRecord deserializeUndoLog(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        LogRecord log = new LogRecord();
        log.logType = TYPE_UNDO;
        log.xid = buffer.getLong();
        log.operationType = buffer.getInt();
        int undoDataLen = buffer.getInt();
        log.undoData = new byte[undoDataLen];
        buffer.get(log.undoData);
        log.logRecordLength = 8 + 4 + 4 + undoDataLen;
        return log;
    }

    public int getSerializedSize() {
        return 1 + 4 + (int) logRecordLength;
    }
}
