package com.tudoujun.distribute.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import io.netty.channel.Channel;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 15:34
 */
public class NetUtils {

    public static String getChannelId(Channel channel) {
        return channel.id().asLongText().replaceAll("-", "");
    }

    private static String HOSTNAME;

    public static String getHostName() {
        if (HOSTNAME == null) {
            try {
                HOSTNAME = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                String host = e.getMessage();
                if (host != null) {
                    int colon = host.indexOf(":");
                    if (colon > 0) {
                        return host.substring(0, colon);
                    }
                }
                HOSTNAME = "UnknownHost";
            }
        }

        return HOSTNAME;
    }
}
