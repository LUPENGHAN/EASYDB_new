package DB.record.models;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Record {
    //  记录结构
    short length;              // 记录长度
    byte status;              // 0: 有效, 1: 删除, 2: 已更新

    long xid;                 // 所属事务 ID

    long beginTS;             // 可选：版本开始时间戳
    long endTS;               // 可选：版本结束时间戳

    long prevVersionPointer; // 上一版本的指针，可为 PageID+Offset 或 UndoLogRef

    byte[] nullBitmap;        // null 位图（变长字段可为空）
    short[] fieldOffsets;     // 每个字段起始位置（变长字段支持）

    byte[] data;              // 实际数据（变长字段拼接）

    // 记录引用
    int pageId;               // 记录所在页面ID
    int slotId;               // 记录所在槽位ID
    
    // 字段值映射（用于高级操作）
    @Builder.Default
    private Map<String, Object> fields = new HashMap<>();
    
    /**
     * 获取字段值
     * @param fieldName 字段名
     * @return 字段值
     */
    public Object getFieldValue(String fieldName) {
        return fields.get(fieldName);
    }
    
    /**
     * 设置字段值
     * @param fieldName 字段名
     * @param value 字段值
     */
    public void setFieldValue(String fieldName, Object value) {
        fields.put(fieldName, value);
    }
    
    // 显式getter/setter方法
    public short getLength() {
        return length;
    }
    
    public void setLength(short length) {
        this.length = length;
    }
    
    public byte getStatus() {
        return status;
    }
    
    public void setStatus(byte status) {
        this.status = status;
    }
    
    public long getXid() {
        return xid;
    }
    
    public void setXid(long xid) {
        this.xid = xid;
    }
    
    public long getBeginTS() {
        return beginTS;
    }
    
    public void setBeginTS(long beginTS) {
        this.beginTS = beginTS;
    }
    
    public long getEndTS() {
        return endTS;
    }
    
    public void setEndTS(long endTS) {
        this.endTS = endTS;
    }
    
    public long getPrevVersionPointer() {
        return prevVersionPointer;
    }
    
    public void setPrevVersionPointer(long prevVersionPointer) {
        this.prevVersionPointer = prevVersionPointer;
    }
    
    public byte[] getNullBitmap() {
        return nullBitmap;
    }
    
    public void setNullBitmap(byte[] nullBitmap) {
        this.nullBitmap = nullBitmap;
    }
    
    public short[] getFieldOffsets() {
        return fieldOffsets;
    }
    
    public void setFieldOffsets(short[] fieldOffsets) {
        this.fieldOffsets = fieldOffsets;
    }
    
    public byte[] getData() {
        return data;
    }
    
    public void setData(byte[] data) {
        this.data = data;
    }
    
    public int getPageId() {
        return pageId;
    }
    
    public void setPageId(int pageId) {
        this.pageId = pageId;
    }
    
    public int getSlotId() {
        return slotId;
    }
    
    public void setSlotId(int slotId) {
        this.slotId = slotId;
    }
    
    public Map<String, Object> getFields() {
        return fields;
    }
    
    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }
}
