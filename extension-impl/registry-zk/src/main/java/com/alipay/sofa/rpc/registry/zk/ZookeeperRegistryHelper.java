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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alipay.sofa.rpc.client.ProviderHelper;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.codec.common.StringSerializer;
import com.alipay.sofa.rpc.common.RpcConfigs;
import com.alipay.sofa.rpc.common.RpcConstants;
import com.alipay.sofa.rpc.common.SystemInfo;
import com.alipay.sofa.rpc.common.utils.BeanUtils;
import com.alipay.sofa.rpc.common.utils.CommonUtils;
import com.alipay.sofa.rpc.common.utils.ReflectUtils;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.AbstractInterfaceConfig;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.registry.utils.RegistryUtils;
import org.apache.curator.framework.recipes.cache.ChildData;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.alipay.sofa.rpc.common.utils.StringUtils.CONTEXT_SEP;

/**
 * Helper for ZookeeperRegistry
 *
 * @author <a href=mailto:zhanggeng.zg@antfin.com>GengZhang</a>
 */
public class ZookeeperRegistryHelper extends RegistryUtils {

    /**
     * Convert url to provider list.
     *
     * @param providerPath
     * @param currentData  the current data
     * @return the list
     * @throws UnsupportedEncodingException decode exception
     */
    static List<ProviderInfo> convertUrlsToProviders(String providerPath,
                                                     List<ChildData> currentData,
                                                     List<ChildData> configuratorData)
        throws UnsupportedEncodingException {
        //todo 2019-07-04 10:43 单元测试
        List<ProviderInfo> providerInfos = new ArrayList<ProviderInfo>();
        if (CommonUtils.isEmpty(currentData)) {
            return providerInfos;
        }
        Map<String, URL> configMap = parseConfig(providerPath, configuratorData);

        for (ChildData childData : currentData) {
            providerInfos.add(mergeConfigToProvider(providerPath, childData, configMap));
        }

        return providerInfos;
    }

    private static Map<String, URL> parseConfig(String providerPath, List<ChildData> configuratorData) throws UnsupportedEncodingException {
        Map<String, URL> result = new HashMap<>();
        String configuratorPath = getConfiguratorPath(providerPath);
        for (ChildData configuratorDatum : configuratorData) {
            String urlStr = configuratorDatum.getPath().substring(configuratorPath.length() + 1);
            urlStr = URLDecoder.decode(urlStr, "UTF-8");
            URL url = UrlUtils.parseURL(urlStr, null);
            result.put(url.getHost() + url.getPort(), url);
        }
        return result;

    }

    private static String getConfiguratorPath(String providerPath) {
        int sepIndex = providerPath.lastIndexOf(CONTEXT_SEP);
        if (sepIndex >= 0) {
            return providerPath.substring(0, sepIndex) + "/configurators";
        }
        return null;
    }

    static ProviderInfo mergeConfigToProvider(String providerPath,
                                              ChildData childData, Map<String, URL> configMap)
        throws UnsupportedEncodingException {
        String url = childData.getPath().substring(providerPath.length() + 1); // 去掉头部
        url = URLDecoder.decode(url, "UTF-8");
        String mergedUrl = getMergedUrl(url, configMap);
        ProviderInfo providerInfo = ProviderHelper.toProviderInfo(mergedUrl);

        processWarmUpWeight(providerInfo);

        return providerInfo;
    }

    private static String getMergedUrl(String url, Map<String, URL> configMap) {
        URL originUrl = UrlUtils.parseURL(url, null);
        URL configuratorUrl = configMap.get(originUrl.getIp() + originUrl.getPort());
        URL newUrl = originUrl;
        if (Optional.ofNullable(configuratorUrl).map(URL::getParameters).isPresent()) {
            for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                newUrl = newUrl.addParameter(entry.getKey(), entry.getValue());

            }
        }

        return newUrl.toFullString();
    }

    /**
     * Convert child data to attribute list.
     *
     * @param configPath  the config path
     * @param currentData the current data
     * @return the attribute list
     */
    static List<Map<String, String>> convertConfigToAttributes(String configPath,
                                                               List<ChildData> currentData) {
        List<Map<String, String>> attributes = new ArrayList<Map<String, String>>();
        if (CommonUtils.isEmpty(currentData)) {
            return attributes;
        }

        for (ChildData childData : currentData) {
            attributes.add(convertConfigToAttribute(configPath, childData, false));
        }
        return attributes;
    }

    /**
     * Convert child data to attribute.
     *
     * @param configPath the config path
     * @param childData  the child data
     * @param removeType is remove type
     * @return the attribute
     */
    static Map<String, String> convertConfigToAttribute(String configPath, ChildData childData,
                                                        boolean removeType) {
        String attribute = childData.getPath().substring(configPath.length() + 1);
        //If event type is CHILD_REMOVED, attribute should return to default value
        return Collections.singletonMap(attribute, removeType ? RpcConfigs.getStringValue(attribute)
            : StringSerializer.decode(childData.getData()));
    }

    /**
     * Convert child data to attribute list.
     *
     * @param config       the interface config
     * @param overridePath the override path
     * @param currentData  the current data
     * @return the attribute list
     * @throws UnsupportedEncodingException decode exception
     */
    static List<Map<String, String>> convertOverrideToAttributes(AbstractInterfaceConfig config,
                                                                 String overridePath,
                                                                 List<ChildData> currentData)
        throws UnsupportedEncodingException {
        List<Map<String, String>> attributes = new ArrayList<Map<String, String>>();
        if (CommonUtils.isEmpty(currentData)) {
            return attributes;
        }

        for (ChildData childData : currentData) {
            String url = URLDecoder.decode(childData.getPath().substring(overridePath.length() + 1),
                "UTF-8");
            if (config instanceof ConsumerConfig) {
                //If child data contains system local host, convert config to attribute
                if (StringUtils.isNotEmpty(url) && StringUtils.isNotEmpty(SystemInfo.getLocalHost())
                    && url.contains("://" + SystemInfo.getLocalHost() + "?")) {
                    attributes.add(convertConfigToAttribute(overridePath, childData, false));
                }
            }
        }
        return attributes;
    }

    /**
     * Convert child data to attribute.
     *
     * @param overridePath    the override path
     * @param childData       the child data
     * @param removeType      is remove type
     * @param interfaceConfig register provider/consumer config
     * @return the attribute
     * @throws Exception decode exception
     */
    static Map<String, String> convertOverrideToAttribute(String overridePath, ChildData childData,
                                                          boolean removeType,
                                                          AbstractInterfaceConfig interfaceConfig) throws Exception {
        String url = URLDecoder.decode(childData.getPath().substring(overridePath.length() + 1),
            "UTF-8");
        Map<String, String> attribute = new ConcurrentHashMap<String, String>();
        for (String keyPairs : url.substring(url.indexOf('?') + 1).split("&")) {
            String[] overrideAttrs = keyPairs.split("=");
            // TODO 这个列表待确认，不少字段是不支持的
            List<String> configKeys = Arrays.asList(RpcConstants.CONFIG_KEY_TIMEOUT,
                RpcConstants.CONFIG_KEY_SERIALIZATION, RpcConstants.CONFIG_KEY_LOADBALANCER);
            if (configKeys.contains(overrideAttrs[0])) {
                if (removeType) {
                    Class clazz = null;
                    if (interfaceConfig instanceof ProviderConfig) {
                        // TODO 服务端也生效？
                        clazz = ProviderConfig.class;
                    } else if (interfaceConfig instanceof ConsumerConfig) {
                        clazz = ConsumerConfig.class;
                    }
                    if (clazz != null) {
                        Method getMethod = ReflectUtils.getPropertyGetterMethod(clazz,
                            overrideAttrs[0]);
                        Class propertyClazz = getMethod.getReturnType();
                        //If event type is CHILD_REMOVED, attribute should return to register value
                        attribute.put(overrideAttrs[0], StringUtils.toString(BeanUtils
                            .getProperty(interfaceConfig, overrideAttrs[0], propertyClazz)));
                    }
                } else {
                    attribute.put(overrideAttrs[0], overrideAttrs[1]);
                }
            }
        }
        return attribute;
    }

    static String buildOverridePath(String rootPath, AbstractInterfaceConfig config) {
        return rootPath + "sofa-rpc/" + config.getInterfaceId() + "/overrides";
    }
}
