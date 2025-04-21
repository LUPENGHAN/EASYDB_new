package DB.record.impl;

import DB.page.interfaces.PageManager;
import DB.page.models.Page;
import DB.record.models.Record;
import DB.record.interfaces.RecordManager;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * 记录管理器实现类
 */
@Getter
@RequiredArgsConstructor
public class RecordManagerImpl implements RecordManager {
    private final PageManager pageManager;
    private static final byte RECORD_STATUS_VALID = 1;
    private static final byte RECORD_STATUS_DELETED = 0;
    private static final byte RECORD_STATUS_INVALID = -1;

    @Override
    public Record insertRecord(Page page, byte[] data, long xid) {
        // 创建新记录
        Record record = new Record();
        record.setLength((short) data.length);
        record.setStatus(RECORD_STATUS_VALID);
        record.setXid(xid);
        record.setBeginTS(System.currentTimeMillis());
        record.setEndTS(Long.MAX_VALUE);
        record.setPrevVersionPointer(-1);
        record.setData(data);
        
        // 设置null位图和字段偏移（这里简化处理，实际应该根据schema计算）
        int fieldCount = 1; // 假设只有一个字段
        int nullBitmapLength = (fieldCount + 7) / 8;
        byte[] nullBitmap = new byte[nullBitmapLength];
        short[] fieldOffsets = new short[fieldCount];
        fieldOffsets[0] = 0; // 第一个字段从0开始
        
        record.setNullBitmap(nullBitmap);
        record.setFieldOffsets(fieldOffsets);
        
        // 添加到页面
        page.getRecords().add(record);
        page.getHeader().setRecordCount(page.getHeader().getRecordCount() + 1);
        page.setDirty(true);
        
        return record;
    }

    @Override
    public Record updateRecord(Page page, Record record, byte[] newData, long xid) {
        // 创建新版本记录
        Record newRecord = new Record();
        newRecord.setLength((short) newData.length);
        newRecord.setStatus(RECORD_STATUS_VALID);
        newRecord.setXid(xid);
        newRecord.setBeginTS(System.currentTimeMillis());
        newRecord.setEndTS(Long.MAX_VALUE);
        newRecord.setPrevVersionPointer(record.getPrevVersionPointer());
        newRecord.setData(newData);
        
        // 复制null位图和字段偏移
        newRecord.setNullBitmap(record.getNullBitmap().clone());
        newRecord.setFieldOffsets(record.getFieldOffsets().clone());
        
        // 更新原记录的结束时间戳和版本指针
        record.setEndTS(System.currentTimeMillis());
        record.setPrevVersionPointer(newRecord.getBeginTS());
        
        // 添加到页面
        page.getRecords().add(newRecord);
        page.getHeader().setRecordCount(page.getHeader().getRecordCount() + 1);
        page.setDirty(true);
        
        return newRecord;
    }

    @Override
    public void deleteRecord(Page page, Record record, long xid) {
        // 标记记录为已删除
        record.setStatus(RECORD_STATUS_DELETED);
        record.setEndTS(System.currentTimeMillis());
        record.setXid(xid);
        page.setDirty(true);
    }

    @Override
    public byte[] readRecord(Page page, Record record) {
        if (!isValidRecord(record)) {
            return null;
        }
        return record.getData();
    }

    @Override
    public List<Record> getAllRecords(Page page) {
        List<Record> validRecords = new ArrayList<>();
        for (Record record : page.getRecords()) {
            if (isValidRecord(record)) {
                validRecords.add(record);
            }
        }
        return validRecords;
    }

    @Override
    public long getRecordVersion(Record record) {
        return record.getBeginTS();
    }

    @Override
    public void setRecordVersion(Record record, long version) {
        record.setBeginTS(version);
    }

    @Override
    public boolean isValidRecord(Record record) {
        return record != null && 
               record.getStatus() == RECORD_STATUS_VALID && 
               System.currentTimeMillis() >= record.getBeginTS() && 
               System.currentTimeMillis() < record.getEndTS();
    }

    @Override
    public byte getRecordStatus(Record record) {
        return record.getStatus();
    }

    @Override
    public void setRecordStatus(Record record, byte status) {
        record.setStatus(status);
    }

    @Override
    public long getRecordXid(Record record) {
        return record.getXid();
    }

    @Override
    public void setRecordXid(Record record, long xid) {
        record.setXid(xid);
    }

    @Override
    public long[] getRecordTimestamps(Record record) {
        return new long[]{record.getBeginTS(), record.getEndTS()};
    }

    @Override
    public void setRecordTimestamps(Record record, long beginTS, long endTS) {
        record.setBeginTS(beginTS);
        record.setEndTS(endTS);
    }

    @Override
    public long getRecordPrevVersionPointer(Record record) {
        return record.getPrevVersionPointer();
    }

    @Override
    public void setRecordPrevVersionPointer(Record record, long pointer) {
        record.setPrevVersionPointer(pointer);
    }

    @Override
    public byte[] getRecordNullBitmap(Record record) {
        return record.getNullBitmap();
    }

    @Override
    public void setRecordNullBitmap(Record record, byte[] nullBitmap) {
        record.setNullBitmap(nullBitmap);
    }

    @Override
    public short[] getRecordFieldOffsets(Record record) {
        return record.getFieldOffsets();
    }

    @Override
    public void setRecordFieldOffsets(Record record, short[] fieldOffsets) {
        record.setFieldOffsets(fieldOffsets);
    }
} 