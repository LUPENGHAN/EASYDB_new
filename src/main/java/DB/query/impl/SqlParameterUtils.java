package DB.query.impl;

import lombok.extern.slf4j.Slf4j;

/**
 * SQL参数处理工具类
 * 提供参数替换等通用功能
 */
@Slf4j
public class SqlParameterUtils {

    /**
     * 将参数应用到SQL语句中
     * @param sql 包含参数占位符的SQL语句
     * @param params 参数值数组
     * @return 替换参数后的SQL语句
     */
    public static String applyParams(String sql, Object[] params) {
        if (params == null || params.length == 0) {
            return sql;
        }

        StringBuilder result = new StringBuilder();
        int lastIndex = 0;
        int paramIndex = 0;

        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?' && paramIndex < params.length) {
                result.append(sql, lastIndex, i);
                Object param = params[paramIndex++];

                // 格式化参数
                result.append(formatParam(param));

                lastIndex = i + 1;
            }
        }

        if (lastIndex < sql.length()) {
            result.append(sql.substring(lastIndex));
        }

        return result.toString();
    }

    /**
     * 将参数格式化为SQL值字符串
     * @param param 参数值
     * @return 格式化后的字符串
     */
    public static String formatParam(Object param) {
        if (param == null) {
            return "NULL";
        } else if (param instanceof String) {
            return "'" + ((String) param).replace("'", "''") + "'";
        } else if (param instanceof java.util.Date) {
            return "'" + param + "'";
        } else {
            return param.toString();
        }
    }

    /**
     * 计算SQL中的参数占位符数量
     * @param sql SQL语句
     * @return 参数数量
     */
    public static int countParams(String sql) {
        int count = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') {
                count++;
            }
        }
        return count;
    }
}