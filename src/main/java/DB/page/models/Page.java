package DB.page.models;

import DB.record.models.Record;
import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * 页面模型类
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Page {
    // Page Head
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageHead {
        int pageId;                  // 页唯一编号
        long fileOffset;            // 在文件中的偏移（物理地址）
        long pageLSN;               // 日志序列号，用于恢复
        byte pageType;              // 页类型：数据页/目录页/undo页
        int prevPageId;            // 上一页页号（可选）
        int nextPageId;            // 下一页页号（可选）
        int freeSpaceDirCount;      // 空闲段数量
        int freeSpacePointer;       // 空闲空间指针
        int slotCount;              // 当前 slot 数量
        int recordCount;           // 有效记录数量
        int checksum;               // 页校验和
        long version;               // 页面版本号（用于MVCC）
        long createTime;            // 页面创建时间
        long lastModifiedTime;      // 最后修改时间
        
        // B+树索引相关字段
        boolean isLeaf;             // 是否为叶子节点
        int keyCount;              // 键的数量

        // 添加缺失的setIsLeaf方法
        public void setIsLeaf(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }
    }

    // 页面类型枚举
    public enum PageType {
        DATA((byte)0),             // 数据页
        INDEX((byte)1),            // 索引页
        UNDO((byte)2);             // UNDO页

        private final byte value;
        PageType(byte value) {
            this.value = value;
        }
        public byte getValue() {
            return value;
        }
    }

    //空闲空间目录项
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SlotDirectoryEntry {
        short offset;               // 记录偏移
        boolean inUse;              // 是否有效（可用于 slot reuse）
        long recordVersion;         // 记录版本号（用于MVCC）
        int pageId;                 // 记录所在页面ID
        int slotId;                 // 记录所在槽位ID
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FreeSpaceEntry {
        int offset;     // 空间起始地址
        int length;     // 空间长度
    }

    // B+树索引相关字段
    private List<Object> keys;      // 键列表
    private List<Page> children;    // 子页面列表
    private List<Record> records;   // 记录列表（仅叶子节点使用）

    // 页内主要结构
    private PageHead header;
    private List<SlotDirectoryEntry> slotDirectory;
    private List<FreeSpaceEntry> freeSpaceDirectory;
    private byte[] freeSpace; // 用于表示空闲区域，也可抽象为 FreeSpaceManager

    // 页面状态
    private boolean isDirty;
    private int pinCount;
    private long lastAccessTime;

    @Builder
    public Page(int pageId) {
        this.header = PageHead.builder()
            .pageId(pageId)
            .createTime(System.currentTimeMillis())
            .lastModifiedTime(System.currentTimeMillis())
            .pageType(PageType.DATA.getValue())
            .freeSpaceDirCount(0)
            .slotCount(0)
            .recordCount(0)
            .isLeaf(true)
            .keyCount(0)
            .build();
        
        this.slotDirectory = new ArrayList<>();
        this.freeSpaceDirectory = new ArrayList<>();
        this.freeSpace = new byte[0];
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.records = new ArrayList<>();
        
        this.isDirty = false;
        this.pinCount = 0;
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 增加页面引用计数
     */
    public void pin() {
        pinCount++;
    }

    /**
     * 减少页面引用计数
     */
    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    /**
     * 更新最后访问时间
     */
    public void updateLastAccessTime() {
        lastAccessTime = System.currentTimeMillis();
    }

    // B+树索引相关方法
    public void addKey(Object key) {
        keys.add(key);
        header.setKeyCount(header.getKeyCount() + 1);
        isDirty = true;
    }

    public void addKey(int index, Object key) {
        keys.add(index, key);
        header.setKeyCount(header.getKeyCount() + 1);
        isDirty = true;
    }

    public void addChild(Page child) {
        children.add(child);
        isDirty = true;
    }

    public void addChild(int index, Page child) {
        children.add(index, child);
        isDirty = true;
    }

    public void addRecord(Record record) {
        records.add(record);
        header.setRecordCount(header.getRecordCount() + 1);
        isDirty = true;
    }

    public void addRecord(int index, Record record) {
        records.add(index, record);
        header.setRecordCount(header.getRecordCount() + 1);
        isDirty = true;
    }

    public Object getKey(int index) {
        return keys.get(index);
    }

    public Page getChild(int index) {
        return children.get(index);
    }

    public Record getRecord(int index) {
        return records.get(index);
    }

    public void setKey(int index, Object key) {
        keys.set(index, key);
        isDirty = true;
    }

    public void setChild(int index, Page child) {
        children.set(index, child);
        isDirty = true;
    }

    public void setRecord(int index, Record record) {
        records.set(index, record);
        isDirty = true;
    }

    public void removeKey(int index) {
        keys.remove(index);
        header.setKeyCount(header.getKeyCount() - 1);
        isDirty = true;
    }

    public void removeChild(int index) {
        children.remove(index);
        isDirty = true;
    }

    public void removeRecord(int index) {
        records.remove(index);
        header.setRecordCount(header.getRecordCount() - 1);
        isDirty = true;
    }

    /**
     * 添加空闲空间项
     */
    public void addFreeSpaceEntry(int offset, int length) {
        freeSpaceDirectory.add(FreeSpaceEntry.builder()
            .offset(offset)
            .length(length)
            .build());
        header.setFreeSpaceDirCount(header.getFreeSpaceDirCount() + 1);
        isDirty = true;
    }

    /**
     * 添加槽位目录项
     */
    public void addSlotDirectoryEntry(SlotDirectoryEntry entry) {
        slotDirectory.add(entry);
        header.setSlotCount(header.getSlotCount() + 1);
        isDirty = true;
    }

    /**
     * 检查是否需要压缩
     */
    public boolean needsCompaction() {
        // 如果碎片化程度超过50%，则需要压缩
        int totalFreeSpace = freeSpaceDirectory.stream()
            .mapToInt(FreeSpaceEntry::getLength)
            .sum();
        return totalFreeSpace > 0 && 
               (double)totalFreeSpace / (records.size() * 100) > 0.5;
    }
}

