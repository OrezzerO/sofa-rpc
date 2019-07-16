/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.rpc.registry.zk;

import com.alipay.sofa.rpc.client.ProviderHelper;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.client.ProviderInfoAttrs;
import com.alipay.sofa.rpc.client.ProviderStatus;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:lw111072@antfin.com">LiWei.Liangen</a>
 */
public class ZookeeperRegistryHelperTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperRegistryHelperTest.class);

    @Test
    public void testWarmup() throws UnsupportedEncodingException, InterruptedException {

        long now = System.currentTimeMillis();

        ProviderInfo providerInfo = new ProviderInfo()
            .setWeight(200)
            .setStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT, "200")
            .setStaticAttr(ProviderInfoAttrs.ATTR_START_TIME, String.valueOf(now))
            .setStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_TIME, String.valueOf(200))
            .setStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT, String.valueOf(700));

        ZookeeperRegistryHelper.processWarmUpWeight(providerInfo);

        Assert.assertEquals("200", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT));
        Assert.assertEquals(now + "", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_START_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));

        Assert.assertEquals(now + 200, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARM_UP_END_TIME));
        Assert.assertEquals(700, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));
        Assert.assertEquals(ProviderStatus.WARMING_UP, providerInfo.getStatus());
        Assert.assertEquals(700, providerInfo.getWeight());

        long elapsed = System.currentTimeMillis() - now;
        LOGGER.info("elapsed time: " + elapsed + "ms");

        long sleepTime = 300 - elapsed;
        if (sleepTime >= 0) {
            Thread.sleep(sleepTime);
        }

        Assert.assertEquals(ProviderStatus.AVAILABLE, providerInfo.getStatus());
        Assert.assertEquals(200, providerInfo.getWeight());
    }

    @Test
    public void testNoWarmupTime() throws InterruptedException {
        long now = System.currentTimeMillis();

        ProviderInfo providerInfo = new ProviderInfo()
            .setWeight(300)
            .setStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT, "300")
            .setStaticAttr(ProviderInfoAttrs.ATTR_START_TIME, String.valueOf(now))
            .setStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT, String.valueOf(800));

        ZookeeperRegistryHelper.processWarmUpWeight(providerInfo);

        Assert.assertEquals("300", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT));
        Assert.assertEquals(now + "", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_START_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));

        Assert.assertEquals(null, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARM_UP_END_TIME));
        Assert.assertEquals(null, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));
        Assert.assertEquals(ProviderStatus.AVAILABLE, providerInfo.getStatus());
        Assert.assertEquals(300, providerInfo.getWeight());
    }

    @Test
    public void testNoWarmupWeight() throws InterruptedException {
        long now = System.currentTimeMillis();

        ProviderInfo providerInfo = new ProviderInfo()
            .setWeight(600)
            .setStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT, "600")
            .setStaticAttr(ProviderInfoAttrs.ATTR_START_TIME, String.valueOf(now))
            .setStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_TIME, String.valueOf(30));

        ZookeeperRegistryHelper.processWarmUpWeight(providerInfo);

        Assert.assertEquals("600", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WEIGHT));
        Assert.assertEquals(now + "", providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_START_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_TIME));
        Assert.assertEquals(null, providerInfo.getStaticAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));

        Assert.assertEquals(null, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARM_UP_END_TIME));
        Assert.assertEquals(null, providerInfo.getDynamicAttr(ProviderInfoAttrs.ATTR_WARMUP_WEIGHT));
        Assert.assertEquals(ProviderStatus.AVAILABLE, providerInfo.getStatus());
        Assert.assertEquals(600, providerInfo.getWeight());
    }

    @Test
    public void testCustomParams() {
        ProviderConfig providerConfig = new ProviderConfig();
        Map<String, String> map = new HashMap<String, String>();
        map.put("x", "y");
        map.put("a", "b");
        providerConfig.setParameters(map);

        ServerConfig server = new ServerConfig();
        providerConfig.setServer(server);
        List<String> urls = ZookeeperRegistryHelper.convertProviderToUrls(providerConfig);
        LOGGER.info(urls.toString());

        Assert.assertNotNull(urls);
        Assert.assertEquals(1, urls.size());

        String url = urls.get(0);

        ProviderInfo providerInfo = ProviderHelper.toProviderInfo(url);

        LOGGER.info(providerInfo.toString());

        Assert.assertEquals("b", providerInfo.getStaticAttr("a"));
        Assert.assertEquals("y", providerInfo.getStaticAttr("x"));

    }

    @Test
    public void testMergeConfig() throws UnsupportedEncodingException {
        String providerPath = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/providers";
        String configuratorPath = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/configurators";
        String providerDataPath1 = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/providers/rest%3A%2F%2F172.20.136.45%3A8888%3Fversion%3D1.0%26accepts%3D100000%26appName%3Dsimple-server%26weight%3D100%26language%3Djava%26pid%3D14074%26starterVersion%3D1.1.7-SNAPSHOT%26interface%3Dcom.ppdai.framework.sofa.simple.api.SimpleApi%26timeout%3D3000%26serialization%3Dhessian2%26protocol%3Drest%26delay%3D-1%26appId%3D10011107%26dynamic%3Dfalse%26startTime%3D1562221505822%26id%3Drpc-cfg-1%26uniqueId%3D%26rpcVer%3D50600";
        String providerDataPath2 = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/providers/rest%3A%2F%2F172.20.136.46%3A8888%3Fversion%3D1.0%26accepts%3D100000%26appName%3Dsimple-server%26weight%3D100%26language%3Djava%26pid%3D14074%26starterVersion%3D1.1.7-SNAPSHOT%26interface%3Dcom.ppdai.framework.sofa.simple.api.SimpleApi%26timeout%3D3000%26serialization%3Dhessian2%26protocol%3Drest%26delay%3D-1%26appId%3D10011107%26dynamic%3Dfalse%26startTime%3D1562221505822%26id%3Drpc-cfg-1%26uniqueId%3D%26rpcVer%3D50600";
        String configuratorDataPath1 = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/configurators/configurator%3a%2f%2f172.20.136.45%3a8888%3fweight%3d45%26up%3d1";
        String configuratorDataPath2 = "/dev/dev/sofa-rpc/com.ppdai.framework.sofa.simple.api.SimpleApi/configurators/configurator%3a%2f%2f172.20.136.46%3a8888%3fweight%3d46";

        ChildData childData1 = new ChildData(providerDataPath1, null, null);
        ChildData childData2 = new ChildData(providerDataPath2, null, null);
        ChildData configData1 = new ChildData(configuratorDataPath1, null, null);
        ChildData configData2 = new ChildData(configuratorDataPath2, null, null);

        ArrayList<ChildData> childDataList = new ArrayList<>(2);
        childDataList.add(childData1);
        childDataList.add(childData2);

        ArrayList<ChildData> configDataList = new ArrayList<>(2);
        configDataList.add(configData1);
        configDataList.add(configData2);


        List<ProviderInfo> providerInfos = ZookeeperRegistryHelper.convertUrlsToProviders(providerPath, childDataList, configDataList);

        for (ProviderInfo providerInfo : providerInfos) {
            Assert.assertTrue(providerInfo.getHost().endsWith(String.valueOf(providerInfo.getWeight())));
            if(providerInfo.getHost().endsWith("45")){
                Assert.assertEquals("1",providerInfo.getAttr("up"));
            }
        }
    }
}