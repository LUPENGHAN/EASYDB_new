package DB;

import DB.record.models.Record;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Record类测试
 */
public class RecordTest {
    
    @Test
    public void testRecordCreation() {
        // 创建记录
        Record record = Record.builder()
                .length((short) 100)
                .status((byte) 1)
                .xid(123456L)
                .beginTS(System.currentTimeMillis())
                .endTS(Long.MAX_VALUE)
                .prevVersionPointer(-1L)
                .nullBitmap(new byte[] {0})
                .fieldOffsets(new short[] {0, 10, 20})
                .data(new byte[100])
                .pageId(1)
                .slotId(5)
                .build();
        
        // 验证记录属性
        assertEquals((short) 100, record.getLength());
        assertEquals((byte) 1, record.getStatus());
        assertEquals(123456L, record.getXid());
        assertTrue(record.getBeginTS() > 0);
        assertEquals(Long.MAX_VALUE, record.getEndTS());
        assertEquals(-1L, record.getPrevVersionPointer());
        assertNotNull(record.getNullBitmap());
        assertEquals(1, record.getNullBitmap().length);
        assertNotNull(record.getFieldOffsets());
        assertEquals(3, record.getFieldOffsets().length);
        assertNotNull(record.getData());
        assertEquals(100, record.getData().length);
        assertEquals(1, record.getPageId());
        assertEquals(5, record.getSlotId());
    }
    
    @Test
    public void testRecordFields() {
        // 创建记录
        Record record = new Record();
        
        // 测试字段值操作
        record.setFieldValue("id", 1);
        record.setFieldValue("name", "Test User");
        record.setFieldValue("age", 30);
        record.setFieldValue("active", true);
        
        // 验证字段值
        assertEquals(1, record.getFieldValue("id"));
        assertEquals("Test User", record.getFieldValue("name"));
        assertEquals(30, record.getFieldValue("age"));
        assertEquals(true, record.getFieldValue("active"));
        
        // 修改字段值
        record.setFieldValue("age", 31);
        assertEquals(31, record.getFieldValue("age"));
        
        // 测试不存在的字段
        assertNull(record.getFieldValue("nonexistent"));
    }
    
    @Test
    public void testRecordStatus() {
        // 创建记录
        Record record = new Record();
        
        // 设置记录状态
        record.setStatus((byte) 0);  // 有效
        assertEquals((byte) 0, record.getStatus());
        
        record.setStatus((byte) 1);  // 删除
        assertEquals((byte) 1, record.getStatus());
        
        record.setStatus((byte) 2);  // 已更新
        assertEquals((byte) 2, record.getStatus());
    }
    
    @Test
    public void testRecordVersioning() {
        // 创建记录
        Record record = new Record();
        
        // 设置版本信息
        long beginTS = System.currentTimeMillis();
        long endTS = beginTS + 10000;
        
        record.setBeginTS(beginTS);
        record.setEndTS(endTS);
        record.setPrevVersionPointer(12345L);
        
        // 验证版本信息
        assertEquals(beginTS, record.getBeginTS());
        assertEquals(endTS, record.getEndTS());
        assertEquals(12345L, record.getPrevVersionPointer());
    }
} 