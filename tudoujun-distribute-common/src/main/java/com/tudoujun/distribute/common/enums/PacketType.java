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
    NAME_NODE_CONTROLLER_VOTE(19, "NameNode投票选举的票据"),
    NAME_NODE_PEER_AWARE(20, "NameNode相互之间发起连接时的感知请求"),
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
