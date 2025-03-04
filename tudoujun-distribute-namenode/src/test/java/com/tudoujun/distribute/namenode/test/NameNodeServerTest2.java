package com.tudoujun.distribute.namenode.test;

import com.tudoujun.distribute.namenode.NameNode;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/03/03 15:12
 */
public class NameNodeServerTest2 {

    public static void main(String[] args) throws Exception {
        NameNodeConfig config = NameNodeConfig.builder()
                .nameNodeId(2)
                .nameNodePeerServers("namenode1:2341:1,namenode2:2342:2,namenode3:2343:3")
                .port(2342)
                .nameNodeApiCoreSize(1)
                .nameNodeApiMaximumPoolSize(2)
                .nameNodeApiQueueSize(128)
                .build();
        NameNode nameNode = new NameNode(config);

        Runtime.getRuntime().addShutdownHook(new Thread(nameNode::shutdown));
        nameNode.start();
    }
}
