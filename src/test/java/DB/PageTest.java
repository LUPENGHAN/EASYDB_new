package DB;

import DB.page.models.Page;
import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
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
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 页面管理测试类
 */
public class PageTest {
    
    private PageManager pageManager;
    private String dbFilePath;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        // 使用临时目录创建测试数据库文件
        dbFilePath = tempDir.resolve("test.db").toString();
        pageManager = new PageManagerImpl(dbFilePath);
    }
    
    @AfterEach
    void tearDown() {
        try {
            // 关闭页面管理器
            if (pageManager != null) {
                try {
                    ((PageManagerImpl)pageManager).close();
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
    @DisplayName("测试页面创建和获取")
    void testCreateAndGetPage() throws IOException {
        // 创建新页面
        Page page = pageManager.createPage();
        assertNotNull(page, "创建的页面不应为空");
        assertTrue(page.getHeader().getPageId() > 0, "页面ID应该是正数");
        
        // 获取页面
        int pageId = page.getHeader().getPageId();
        Page retrievedPage = pageManager.readPage(pageId);
        assertNotNull(retrievedPage, "获取的页面不应为空");
        assertEquals(pageId, retrievedPage.getHeader().getPageId(), "页面ID应匹配");
    }
    
    @Test
    @DisplayName("测试页面写入和刷新")
    void testWriteAndFlushPage() throws IOException {
        // 创建页面
        Page page = pageManager.createPage();
        int pageId = page.getHeader().getPageId();
        
        // 修改页面
        page.setDirty(true);
        
        // 添加测试记录
        Record record = Record.builder()
                .pageId(pageId)
                .slotId(1)
                .data(new byte[]{1, 2, 3, 4, 5})
                .status((byte)0)  // 设置status为有效
                .length((short)5) // 设置length字段
                .build();
        page.addRecord(record);
        
        // 刷新页面
        pageManager.writePage(page);
        
        // 重新获取页面，检查记录是否保存
        Page retrievedPage = pageManager.readPage(pageId);
        assertEquals(1, retrievedPage.getHeader().getRecordCount(), "记录数应为1");
        assertEquals(pageId, retrievedPage.getRecords().get(0).getPageId(), "记录的页面ID应匹配");
    }
    
    @Test
    @DisplayName("测试页面钉住和取消钉住")
    void testPinAndUnpinPage() {
        // 创建页面
        Page page = pageManager.createPage();
        int pageId = page.getHeader().getPageId();
        
        // 钉住页面
        page.pin();
        assertEquals(1, page.getPinCount(), "钉住计数应为1");
        
        // 再次钉住
        page.pin();
        assertEquals(2, page.getPinCount(), "钉住计数应为2");
        
        // 取消钉住
        page.unpin();
        assertEquals(1, page.getPinCount(), "钉住计数应为1");
        
        // 再次取消钉住
        page.unpin();
        assertEquals(0, page.getPinCount(), "钉住计数应为0");
    }
    
    @Test
    @DisplayName("测试获取脏页")
    void testGetDirtyPages() {
        // 创建多个页面
        Page page1 = pageManager.createPage();
        Page page2 = pageManager.createPage();
        Page page3 = pageManager.createPage();
        
        // 标记部分页面为脏页
        page1.setDirty(true);
        page3.setDirty(true);
        
        // 获取所有缓存中的脏页
        List<Page> dirtyPages = new ArrayList<>();
        for (Page page : ((PageManagerImpl)pageManager).getPageCache().values()) {
            if (page.isDirty()) {
                dirtyPages.add(page);
            }
        }
        
        // 验证脏页数量
        assertEquals(2, dirtyPages.size(), "应有2个脏页");
        
        // 验证脏页ID
        boolean foundPage1 = false;
        boolean foundPage3 = false;
        for (Page page : dirtyPages) {
            if (page.getHeader().getPageId() == page1.getHeader().getPageId()) {
                foundPage1 = true;
            }
            if (page.getHeader().getPageId() == page3.getHeader().getPageId()) {
                foundPage3 = true;
            }
        }
        
        assertTrue(foundPage1, "脏页列表应包含页面1");
        assertTrue(foundPage3, "脏页列表应包含页面3");
    }
    
    @Test
    @DisplayName("测试页面数据结构和功能")
    void testPageStructureAndFunctions() {
        // 创建页面
        Page page = new Page(100);
        
        // 设置页面属性
        page.getHeader().setPageType(Page.PageType.DATA.getValue());
        page.getHeader().setRecordCount(0);
        page.getHeader().setSlotCount(0);
        
        // 测试页面钉住和取消钉住
        assertEquals(0, page.getPinCount(), "初始钉住计数应为0");
        page.pin();
        assertEquals(1, page.getPinCount(), "钉住后计数应为1");
        page.unpin();
        assertEquals(0, page.getPinCount(), "取消钉住后计数应为0");
        
        // 测试脏标记
        assertFalse(page.isDirty(), "初始脏标记应为false");
        page.setDirty(true);
        assertTrue(page.isDirty(), "设置后脏标记应为true");
        
        // 测试添加记录
        Record record1 = Record.builder()
                .pageId(100)
                .slotId(1)
                .status((byte)0)
                .length((short)0)
                .build();
        
        Record record2 = Record.builder()
                .pageId(100)
                .slotId(2)
                .status((byte)0)
                .length((short)0)
                .build();
        
        page.addRecord(record1);
        page.addRecord(record2);
        
        assertEquals(2, page.getHeader().getRecordCount(), "记录数应为2");
        assertEquals(record1, page.getRecord(0), "第一条记录应匹配");
        assertEquals(record2, page.getRecord(1), "第二条记录应匹配");
        
        // 测试槽位目录
        Page.SlotDirectoryEntry slot1 = Page.SlotDirectoryEntry.builder()
                .offset((short)0)
                .inUse(true)
                .pageId(100)
                .slotId(1)
                .build();
        
        page.addSlotDirectoryEntry(slot1);
        assertEquals(1, page.getHeader().getSlotCount(), "槽位数应为1");
    }
} 