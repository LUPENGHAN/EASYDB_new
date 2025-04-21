package DB.page.impl;

import DB.page.interfaces.PageManager;
import DB.page.models.Page;
import DB.record.models.Record;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 页面管理器实现类
 */
@Getter
@RequiredArgsConstructor
public class PageManagerImpl implements PageManager {
    private final String dataFilePath;
    private final RandomAccessFile dataFile;
    private final Map<Integer, Page> pageCache;
    private final AtomicInteger nextPageId;
    private static final int MAX_CACHE_SIZE = 1000;
    private static final int PAGE_SIZE = 4096; // 4KB页面大小

    /**
     * 构造函数
     * @param dataFilePath 数据文件路径
     * @throws IOException 如果文件操作失败
     */
    public PageManagerImpl(String dataFilePath) throws IOException {
        this.dataFilePath = dataFilePath;
        File file = new File(dataFilePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.dataFile = new RandomAccessFile(file, "rw");
        this.pageCache = new HashMap<>();
        this.nextPageId = new AtomicInteger(0);
    }

    @Override
    public Page createPage() {
        // 确保pageId是正数，从1开始递增
        int pageId = nextPageId.incrementAndGet(); // 使用incrementAndGet()而不是getAndIncrement()
        Page page = new Page(pageId);
        pageCache.put(pageId, page);
        return page;
    }

    @Override
    public Page readPage(int pageId) throws IOException {
        // 检查缓存
        Page page = pageCache.get(pageId);
        if (page != null) {
            page.updateLastAccessTime();
            return page;
        }

        // 从文件读取
        long offset = (long) pageId * PAGE_SIZE;
        if (offset >= dataFile.length()) {
            return null;
        }

        dataFile.seek(offset);
        byte[] pageData = new byte[PAGE_SIZE];
        dataFile.readFully(pageData);

        // 解析页面数据
        page = parsePageData(pageId, pageData);
        page.setDirty(false);

        // 更新缓存
        if (pageCache.size() >= MAX_CACHE_SIZE) {
            evictPage();
        }
        pageCache.put(pageId, page);

        return page;
    }

    @Override
    public void writePage(Page page) throws IOException {
        if (!page.isDirty()) {
            return;
        }

        long offset = (long) page.getHeader().getPageId() * PAGE_SIZE;
        dataFile.seek(offset);
        byte[] pageData = serializePage(page);
        dataFile.write(pageData);
        page.setDirty(false);
    }

    @Override
    public int allocatePageId() {
        return nextPageId.getAndIncrement();
    }

    @Override
    public int getTotalPages() {
        return nextPageId.get();
    }

    @Override
    public byte getPageType(Page page) {
        return page.getHeader().getPageType();
    }

    @Override
    public void setPageType(Page page, byte type) {
        page.getHeader().setPageType(type);
        page.setDirty(true);
    }

    @Override
    public int getFreeSpace(Page page) {
        return page.getFreeSpaceDirectory().stream()
            .mapToInt(Page.FreeSpaceEntry::getLength)
            .sum();
    }

    @Override
    public void compactPage(Page page) {
        // 实现页面压缩逻辑
        // 1. 合并相邻的空闲空间
        // 2. 重新组织记录
        // 3. 更新槽位目录
        page.setDirty(true);
    }

    @Override
    public boolean needsCompaction(Page page) {
        return page.needsCompaction();
    }

    @Override
    public int getRecordCount(Page page) {
        return page.getHeader().getRecordCount();
    }

    @Override
    public int getSlotCount(Page page) {
        return page.getHeader().getSlotCount();
    }

    @Override
    public int getFreeSpaceDirCount(Page page) {
        return page.getHeader().getFreeSpaceDirCount();
    }

    @Override
    public void setPageLSN(Page page, long lsn) {
        page.getHeader().setPageLSN(lsn);
        page.setDirty(true);
    }

    @Override
    public long getPageLSN(Page page) {
        return page.getHeader().getPageLSN();
    }

    /**
     * 从缓存中淘汰一个页面
     */
    private void evictPage() {
        // 简单的LRU策略
        long oldestTime = Long.MAX_VALUE;
        int pageIdToEvict = -1;

        for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            if (page.getPinCount() == 0 && page.getLastAccessTime() < oldestTime) {
                oldestTime = page.getLastAccessTime();
                pageIdToEvict = entry.getKey();
            }
        }

        if (pageIdToEvict != -1) {
            Page page = pageCache.remove(pageIdToEvict);
            if (page.isDirty()) {
                try {
                    writePage(page);
                } catch (IOException e) {
                    // 处理写入失败的情况
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 解析页面数据
     */
    private Page parsePageData(int pageId, byte[] pageData) {
        ByteBuffer buffer = ByteBuffer.wrap(pageData);
        
        // 创建页面对象
        Page page = new Page(pageId);
        Page.PageHead header = page.getHeader();
        
        // 解析页面头部
        header.setPageId(buffer.getInt());
        header.setFileOffset(buffer.getLong());
        header.setPageLSN(buffer.getLong());
        header.setPageType(buffer.get());
        header.setPrevPageId(buffer.getInt());
        header.setNextPageId(buffer.getInt());
        header.setFreeSpaceDirCount(buffer.getInt());
        header.setFreeSpacePointer(buffer.getInt());
        header.setSlotCount(buffer.getInt());
        header.setRecordCount(buffer.getInt());
        header.setChecksum(buffer.getInt());
        header.setVersion(buffer.getLong());
        header.setCreateTime(buffer.getLong());
        header.setLastModifiedTime(buffer.getLong());
        
        // 解析槽位目录
        int slotCount = header.getSlotCount();
        List<Page.SlotDirectoryEntry> slotDirectory = new ArrayList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            Page.SlotDirectoryEntry entry = new Page.SlotDirectoryEntry();
            entry.setOffset(buffer.getShort());
            entry.setInUse(buffer.get() == 1);
            entry.setRecordVersion(buffer.getLong());
            entry.setPageId(buffer.getInt());
            entry.setSlotId(buffer.getInt());
            slotDirectory.add(entry);
        }
        page.setSlotDirectory(slotDirectory);
        
        // 解析空闲空间目录
        int freeSpaceDirCount = header.getFreeSpaceDirCount();
        List<Page.FreeSpaceEntry> freeSpaceDirectory = new ArrayList<>(freeSpaceDirCount);
        for (int i = 0; i < freeSpaceDirCount; i++) {
            int offset = buffer.getInt();
            int length = buffer.getInt();
            freeSpaceDirectory.add(new Page.FreeSpaceEntry(offset, length));
        }
        page.setFreeSpaceDirectory(freeSpaceDirectory);
        
        // 解析记录
        int recordCount = header.getRecordCount();
        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            Record record = new Record();
            record.setLength(buffer.getShort());
            record.setStatus(buffer.get());
            record.setXid(buffer.getLong());
            record.setBeginTS(buffer.getLong());
            record.setEndTS(buffer.getLong());
            record.setPrevVersionPointer(buffer.getLong());
            
            // 读取null位图
            int nullBitmapLength = (recordCount + 7) / 8;
            byte[] nullBitmap = new byte[nullBitmapLength];
            buffer.get(nullBitmap);
            record.setNullBitmap(nullBitmap);
            
            // 读取字段偏移
            int fieldCount = buffer.getShort();
            short[] fieldOffsets = new short[fieldCount];
            for (int j = 0; j < fieldCount; j++) {
                fieldOffsets[j] = buffer.getShort();
            }
            record.setFieldOffsets(fieldOffsets);
            
            // 读取记录数据
            byte[] data = new byte[record.getLength()];
            buffer.get(data);
            record.setData(data);
            
            // 设置记录引用
            record.setPageId(pageId);
            record.setSlotId(i);
            
            records.add(record);
        }
        page.setRecords(records);
        
        return page;
    }

    /**
     * 序列化页面数据
     */
    private byte[] serializePage(Page page) {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        Page.PageHead header = page.getHeader();
        
        // 序列化页面头部
        buffer.putInt(header.getPageId());
        buffer.putLong(header.getFileOffset());
        buffer.putLong(header.getPageLSN());
        buffer.put(header.getPageType());
        buffer.putInt(header.getPrevPageId());
        buffer.putInt(header.getNextPageId());
        buffer.putInt(header.getFreeSpaceDirCount());
        buffer.putInt(header.getFreeSpacePointer());
        buffer.putInt(header.getSlotCount());
        buffer.putInt(header.getRecordCount());
        buffer.putInt(header.getChecksum());
        buffer.putLong(header.getVersion());
        buffer.putLong(header.getCreateTime());
        buffer.putLong(header.getLastModifiedTime());
        
        // 序列化槽位目录
        for (Page.SlotDirectoryEntry entry : page.getSlotDirectory()) {
            buffer.putShort(entry.getOffset());
            buffer.put((byte)(entry.isInUse() ? 1 : 0));
            buffer.putLong(entry.getRecordVersion());
            buffer.putInt(entry.getPageId());
            buffer.putInt(entry.getSlotId());
        }
        
        // 序列化空闲空间目录
        for (Page.FreeSpaceEntry entry : page.getFreeSpaceDirectory()) {
            buffer.putInt(entry.getOffset());
            buffer.putInt(entry.getLength());
        }
        
        // 序列化记录
        for (Record record : page.getRecords()) {
            buffer.putShort(record.getLength());
            buffer.put(record.getStatus());
            buffer.putLong(record.getXid());
            buffer.putLong(record.getBeginTS());
            buffer.putLong(record.getEndTS());
            buffer.putLong(record.getPrevVersionPointer());
            
            // 写入null位图（确保不为null）
            byte[] nullBitmap = record.getNullBitmap();
            if (nullBitmap == null) {
                // 如果nullBitmap为null，创建一个空位图（全为0，表示没有NULL值）
                int nullBitmapLength = (page.getRecords().size() + 7) / 8;
                nullBitmap = new byte[nullBitmapLength > 0 ? nullBitmapLength : 1];
            }
            buffer.put(nullBitmap);
            
            // 写入字段偏移（确保不为null）
            short[] fieldOffsets = record.getFieldOffsets();
            if (fieldOffsets == null) {
                fieldOffsets = new short[0];
            }
            buffer.putShort((short)fieldOffsets.length);
            for (short offset : fieldOffsets) {
                buffer.putShort(offset);
            }
            
            // 写入记录数据（确保不为null）
            byte[] data = record.getData();
            if (data == null) {
                // 如果数据为null，使用空数组
                data = new byte[0];
            }
            buffer.put(data);
        }
        
        // 填充剩余空间
        while (buffer.position() < PAGE_SIZE) {
            buffer.put((byte)0);
        }
        
        return buffer.array();
    }

    /**
     * 关闭页面管理器
     */
    public void close() throws IOException {
        // 将所有脏页写回磁盘
        for (Page page : pageCache.values()) {
            if (page.isDirty()) {
                writePage(page);
            }
        }
        dataFile.close();
    }
}
