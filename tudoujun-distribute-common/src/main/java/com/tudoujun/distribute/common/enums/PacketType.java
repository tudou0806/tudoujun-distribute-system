package com.tudoujun.distribute.common.enums;

import java.util.Arrays;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/19 16:09
 */
@Getter
@AllArgsConstructor
public enum PacketType {

    UNKNOWN(0, "未知的包类型"),
    ;

    private final Integer value;
    private final String description;

    public static PacketType of(int value) {
        return Arrays.stream(values())
                .filter(t -> t.getValue().equals(value))
                .findFirst()
                .orElse(UNKNOWN);
    }
}
