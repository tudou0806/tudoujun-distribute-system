package com.tudoujun.distribute.namenode.config;

import org.apache.commons.lang3.StringUtils;

import lombok.Builder;
import lombok.Data;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 13:40
 */
@Data
@Builder
public class NameNodeConfig {

    private int port;
    private int nameNodeId;
    private String nameNodePeerServers;

    private int nameNodeApiCoreSize;
    private int nameNodeApiMaximumPoolSize;
    private int nameNodeApiQueueSize;

    public int numOfNode() {
        return StringUtils.isBlank(nameNodePeerServers) ? 1 : nameNodePeerServers.split(",").length;
    }
}
