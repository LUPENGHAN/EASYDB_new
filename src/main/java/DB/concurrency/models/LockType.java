package DB.concurrency.models;

/**
 * 锁类型枚举
 */
public enum LockType {
    SHARED(0),      // 共享锁
    EXCLUSIVE(1);   // 排他锁

    private final int value;

    LockType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static LockType fromValue(int value) {
        for (LockType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid lock type value: " + value);
    }
} 