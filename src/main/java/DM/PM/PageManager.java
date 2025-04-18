package DM.PM;


import DM.PM.models.PageID;

/**
 * 页面管理器接口扩展，增加pin/unpin方法
 */
public interface PageManager {
    /**
     * 读取页面数据
     *
     * @param pageID 页面ID
     * @param offset 页内偏移量
     * @param length 读取长度
     * @return 读取的数据
     */
    byte[] read(PageID pageID, int offset, int length);

    /**
     * 写入页面数据
     *
     * @param pageID 页面ID
     * @param offset 页内偏移量
     * @param data   要写入的数据
     * @param xid    事务ID
     * @return 操作是否成功
     */
    boolean write(PageID pageID, int offset, byte[] data, long xid);

    /**
     * 分配新页面
     *
     * @return 新页面的ID，如果无法分配则返回null
     */
    PageID allocatePage();

    /**
     * 释放页面
     *
     * @param pageID 要释放的页面ID
     * @return 操作是否成功
     */
    boolean freePage(PageID pageID);

    /**
     * 将页面固定在内存中
     *
     * @param pageID 页面ID
     * @return 固定的页面，如果无法固定则返回null
     */
    Page pinPage(PageID pageID);

    /**
     * 解除页面的固定状态
     *
     * @param pageID  页面ID
     * @param isDirty 是否标记为脏页
     */
    void unpinPage(PageID pageID, boolean isDirty);

    /**
     * 刷新所有脏页到磁盘
     */
    void flushAll();

    /**
     * 刷新指定页面到磁盘
     *
     * @param pageID 要刷新的页面ID
     */
    void flushPage(PageID pageID);

    /**
     * 创建检查点
     */
    void checkpoint();

    /**
     * 根据日志进行恢复操作
     */
    void recover();

    /**
     * 关闭页面管理器
     */
    void close();
}