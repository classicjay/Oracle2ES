package com.example.Oracle2ES.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * <p>Title: BONC -  GenerateClient</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
public class GenerateClient {
    private static Settings settings = Settings.builder()
            .put("cluster.name", "Logs_Collect_V1")//设置ES实例的名称
            .put("client.transport.sniff", true)//自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
            .build();

    public static TransportClient getClient()throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.249.216.108"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.249.216.109"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.249.216.110"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.249.216.111"), 9300));
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.31.2"), 9300));
        return client;
    }
}
