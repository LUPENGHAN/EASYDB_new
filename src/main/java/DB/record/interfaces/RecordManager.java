package DB.record.interfaces;

import DB.page.models.Page;
import DB.record.models.Record;
import java.util.List;

/**
 * 记录管理器接口
 */
public interface RecordManager {
    /**
     * 插入新记录
     * @param page 页面
     * @param data 记录数据
     * @param xid 事务ID
     * @return 新创建的记录
     */
    Record insertRecord(Page page, byte[] data, long xid);

    /**
     * 更新记录
     * @param page 页面
     * @param record 要更新的记录
     * @param newData 新数据
     * @param xid 事务ID
     * @return 更新后的记录
     */
    Record updateRecord(Page page, Record record, byte[] newData, long xid);

    /**
     * 删除记录
     * @param page 页面
     * @param record 要删除的记录
     * @param xid 事务ID
     */
    void deleteRecord(Page page, Record record, long xid);

    /**
     * 读取记录数据
     * @param page 页面
     * @param record 记录
     * @return 记录数据
     */
    byte[] readRecord(Page page, Record record);

    /**
     * 获取页面所有记录
     * @param page 页面
     * @return 记录列表
     */
    List<Record> getAllRecords(Page page);

    /**
     * 获取记录版本号
     * @param record 记录
     * @return 版本号
     */
    long getRecordVersion(Record record);

    /**
     * 设置记录版本号
     * @param record 记录
     * @param version 版本号
     */
    void setRecordVersion(Record record, long version);

    /**
     * 检查记录是否有效
     * @param record 记录
     * @return 是否有效
     */
    boolean isValidRecord(Record record);

    /**
     * 获取记录状态
     * @param record 记录
     * @return 记录状态
     */
    byte getRecordStatus(Record record);

    /**
     * 设置记录状态
     * @param record 记录
     * @param status 记录状态
     */
    void setRecordStatus(Record record, byte status);

    /**
     * 获取记录的事务ID
     * @param record 记录
     * @return 事务ID
     */
    long getRecordXid(Record record);

    /**
     * 设置记录的事务ID
     * @param record 记录
     * @param xid 事务ID
     */
    void setRecordXid(Record record, long xid);

    /**
     * 获取记录的版本时间戳
     * @param record 记录
     * @return 版本时间戳
     */
    long[] getRecordTimestamps(Record record);

    /**
     * 设置记录的版本时间戳
     * @param record 记录
     * @param beginTS 开始时间戳
     * @param endTS 结束时间戳
     */
    void setRecordTimestamps(Record record, long beginTS, long endTS);

    /**
     * 获取记录的上一版本指针
     * @param record 记录
     * @return 上一版本指针
     */
    long getRecordPrevVersionPointer(Record record);

    /**
     * 设置记录的上一版本指针
     * @param record 记录
     * @param pointer 上一版本指针
     */
    void setRecordPrevVersionPointer(Record record, long pointer);

    /**
     * 获取记录的null位图
     * @param record 记录
     * @return null位图
     */
    byte[] getRecordNullBitmap(Record record);

    /**
     * 设置记录的null位图
     * @param record 记录
     * @param nullBitmap null位图
     */
    void setRecordNullBitmap(Record record, byte[] nullBitmap);

    /**
     * 获取记录的字段偏移
     * @param record 记录
     * @return 字段偏移数组
     */
    short[] getRecordFieldOffsets(Record record);

    /**
     * 设置记录的字段偏移
     * @param record 记录
     * @param fieldOffsets 字段偏移数组
     */
    void setRecordFieldOffsets(Record record, short[] fieldOffsets);
} 