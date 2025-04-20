package DB;

import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
import DB.page.models.Page;
import DB.record.interfaces.RecordManager;
import DB.record.impl.RecordManagerImpl;
import DB.record.models.Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 记录管理器测试类
 */
public class RecordManagerTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private String dbFilePath;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        // 使用临时目录创建测试数据库文件
        dbFilePath = tempDir.resolve("test.db").toString();
        pageManager = new PageManagerImpl(dbFilePath);
        recordManager = new RecordManagerImpl(pageManager);
    }
    
    @AfterEach
    void tearDown() {
        try {
            // 关闭页面管理器
            if (pageManager instanceof PageManagerImpl) {
                try {
                    ((PageManagerImpl) pageManager).close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            // 确保所有资源释放
            System.gc();
            Thread.sleep(100);
            
            // 删除测试数据库文件
            File dbFile = new File(dbFilePath);
            if (dbFile.exists()) {
                boolean deleted = dbFile.delete();
                if (!deleted) {
                    System.err.println("警告：无法删除数据库文件 " + dbFilePath);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    @DisplayName("测试记录插入")
    void testInsertRecord() {
        // 创建页面
        Page page = pageManager.createPage();
        
        // 创建记录数据
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        long xid = 1001;
        
        // 插入记录
        Record record = recordManager.insertRecord(page, data, xid);
        
        // 验证记录属性
        assertNotNull(record, "插入的记录不应为空");
        assertEquals(xid, record.getXid(), "记录事务ID应匹配");
        assertEquals(data.length, record.getLength(), "记录长度应匹配");
        assertArrayEquals(data, record.getData(), "记录数据应匹配");
        assertTrue(recordManager.isValidRecord(record), "记录应有效");
    }
    
    @Test
    @DisplayName("测试记录更新")
    void testUpdateRecord() {
        // 创建页面并插入记录
        Page page = pageManager.createPage();
        byte[] originalData = new byte[]{1, 2, 3, 4, 5};
        long xid1 = 1001;
        Record originalRecord = recordManager.insertRecord(page, originalData, xid1);
        
        // 更新记录
        byte[] newData = new byte[]{6, 7, 8, 9, 10};
        long xid2 = 1002;
        Record updatedRecord = recordManager.updateRecord(page, originalRecord, newData, xid2);
        
        // 验证更新结果
        assertNotNull(updatedRecord, "更新后的记录不应为空");
        assertEquals(xid2, updatedRecord.getXid(), "更新后记录事务ID应匹配");
        assertEquals(newData.length, updatedRecord.getLength(), "更新后记录长度应匹配");
        assertArrayEquals(newData, updatedRecord.getData(), "更新后记录数据应匹配");
        assertTrue(recordManager.isValidRecord(updatedRecord), "更新后记录应有效");
        
        // 原记录的版本信息应被更新
        assertNotEquals(Long.MAX_VALUE, originalRecord.getEndTS(), "原记录结束时间戳应被更新");
        assertNotEquals(-1, originalRecord.getPrevVersionPointer(), "原记录前版本指针应被更新");
    }
    
    @Test
    @DisplayName("测试记录删除")
    void testDeleteRecord() {
        // 创建页面并插入记录
        Page page = pageManager.createPage();
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        long xid1 = 1001;
        Record record = recordManager.insertRecord(page, data, xid1);
        
        // 删除记录
        long xid2 = 1002;
        recordManager.deleteRecord(page, record, xid2);
        
        // 验证记录被标记为删除
        assertFalse(recordManager.isValidRecord(record), "被删除的记录应无效");
        assertEquals(xid2, record.getXid(), "被删除记录的事务ID应更新为删除事务ID");
        assertNotEquals(Long.MAX_VALUE, record.getEndTS(), "被删除记录的结束时间戳应被更新");
    }
    
    @Test
    @DisplayName("测试记录读取")
    void testReadRecord() {
        // 创建页面并插入记录
        Page page = pageManager.createPage();
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        long xid = 1001;
        Record record = recordManager.insertRecord(page, data, xid);
        
        // 读取记录
        byte[] readData = recordManager.readRecord(page, record);
        
        // 验证读取结果
        assertNotNull(readData, "读取的数据不应为空");
        assertArrayEquals(data, readData, "读取的数据应与原始数据匹配");
    }
    
    @Test
    @DisplayName("测试获取页面所有记录")
    void testGetAllRecords() {
        // 创建页面
        Page page = pageManager.createPage();
        
        // 插入多条记录
        byte[] data1 = new byte[]{1, 2, 3};
        byte[] data2 = new byte[]{4, 5, 6};
        byte[] data3 = new byte[]{7, 8, 9};
        
        Record record1 = recordManager.insertRecord(page, data1, 1001);
        Record record2 = recordManager.insertRecord(page, data2, 1002);
        Record record3 = recordManager.insertRecord(page, data3, 1003);
        
        // 删除一条记录
        recordManager.deleteRecord(page, record2, 1004);
        
        // 获取所有有效记录
        List<Record> records = recordManager.getAllRecords(page);
        
        // 验证结果
        assertEquals(2, records.size(), "应有2条有效记录");
        assertTrue(records.contains(record1), "结果应包含记录1");
        assertTrue(records.contains(record3), "结果应包含记录3");
        assertFalse(records.contains(record2), "结果不应包含已删除的记录2");
    }
    
    @Test
    @DisplayName("测试记录元数据操作")
    void testRecordMetadataOperations() {
        // 创建记录
        Record record = new Record();
        
        // 测试状态设置和获取
        byte status = 1;
        recordManager.setRecordStatus(record, status);
        assertEquals(status, recordManager.getRecordStatus(record), "记录状态应匹配");
        
        // 测试事务ID设置和获取
        long xid = 1005;
        recordManager.setRecordXid(record, xid);
        assertEquals(xid, recordManager.getRecordXid(record), "记录事务ID应匹配");
        
        // 测试版本号设置和获取
        long version = System.currentTimeMillis();
        recordManager.setRecordVersion(record, version);
        assertEquals(version, recordManager.getRecordVersion(record), "记录版本号应匹配");
        
        // 测试时间戳设置和获取
        long beginTS = System.currentTimeMillis();
        long endTS = beginTS + 10000;
        recordManager.setRecordTimestamps(record, beginTS, endTS);
        long[] timestamps = recordManager.getRecordTimestamps(record);
        assertEquals(beginTS, timestamps[0], "开始时间戳应匹配");
        assertEquals(endTS, timestamps[1], "结束时间戳应匹配");
        
        // 测试前版本指针设置和获取
        long pointer = 12345;
        recordManager.setRecordPrevVersionPointer(record, pointer);
        assertEquals(pointer, recordManager.getRecordPrevVersionPointer(record), "前版本指针应匹配");
        
        // 测试Null位图设置和获取
        byte[] nullBitmap = new byte[]{(byte)0xFF};
        recordManager.setRecordNullBitmap(record, nullBitmap);
        assertArrayEquals(nullBitmap, recordManager.getRecordNullBitmap(record), "Null位图应匹配");
        
        // 测试字段偏移设置和获取
        short[] fieldOffsets = new short[]{0, 10, 20, 30};
        recordManager.setRecordFieldOffsets(record, fieldOffsets);
        assertArrayEquals(fieldOffsets, recordManager.getRecordFieldOffsets(record), "字段偏移应匹配");
    }
} 