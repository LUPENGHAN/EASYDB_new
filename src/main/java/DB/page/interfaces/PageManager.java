package DB.page.interfaces;

import DB.page.models.Page;
import java.io.IOException;

/**
 * 页面管理器接口
 */
public interface PageManager {
    /**
     * 创建新页面
     * @return 新创建的页面
     */
    Page createPage();

    /**
     * 读取页面
     * @param pageId 页面ID
     * @return 页面对象
     * @throws IOException 如果读取失败
     */
    Page readPage(int pageId) throws IOException;

    /**
     * 写入页面
     * @param page 要写入的页面
     * @throws IOException 如果写入失败
     */
    void writePage(Page page) throws IOException;

    /**
     * 分配新的页面ID
     * @return 新的页面ID
     */
    int allocatePageId();

    /**
     * 获取页面总数
     * @return 页面总数
     */
    int getTotalPages();

    /**
     * 获取页面类型
     * @param page 页面
     * @return 页面类型
     */
    byte getPageType(Page page);

    /**
     * 设置页面类型
     * @param page 页面
     * @param type 页面类型
     */
    void setPageType(Page page, byte type);

    /**
     * 获取页面空闲空间
     * @param page 页面
     * @return 空闲空间大小
     */
    int getFreeSpace(Page page);

    /**
     * 压缩页面
     * @param page 要压缩的页面
     */
    void compactPage(Page page);

    /**
     * 检查页面是否需要压缩
     * @param page 要检查的页面
     * @return 是否需要压缩
     */
    boolean needsCompaction(Page page);

    /**
     * 获取页面记录数
     * @param page 页面
     * @return 记录数
     */
    int getRecordCount(Page page);

    /**
     * 获取页面槽位数
     * @param page 页面
     * @return 槽位数
     */
    int getSlotCount(Page page);

    /**
     * 获取页面空闲段数
     * @param page 页面
     * @return 空闲段数
     */
    int getFreeSpaceDirCount(Page page);

    /**
     * 设置页面LSN
     * @param page 页面
     * @param lsn 日志序列号
     */
    void setPageLSN(Page page, long lsn);

    /**
     * 获取页面LSN
     * @param page 页面
     * @return 日志序列号
     */
    long getPageLSN(Page page);
} 