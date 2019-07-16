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
package com.alipay.sofa.rpc.client.router;

import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.client.Router;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcRuntimeException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.filter.AutoActive;
import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 2019-07-04.
 *
 * @author Chengxi Zhang
 */
@Extension(value = "upFilter", order = -16000)
@AutoActive(consumerSide = true)
public class TrafficFilterRouter extends Router {

    public static final String   UP_KEY      = "up";
    public static final String   UP_VALUE_UP = "1";
    private static final Logger  log         = LoggerFactory.getLogger(TrafficFilterRouter.class);
    private static AtomicBoolean FORCE_CLOSE = new AtomicBoolean(false);

    public static void setForceClose(boolean forceClose) {
        FORCE_CLOSE.set(forceClose);
    }

    @Override
    public List<ProviderInfo> route(SofaRequest request, List<ProviderInfo> providerInfos) {
        if (FORCE_CLOSE.get() || providerInfos == null || providerInfos.isEmpty()) {
            return providerInfos;
        }

        List<ProviderInfo> upProviders = new ArrayList<>(providerInfos.size());
        for (ProviderInfo providerInfo : providerInfos) {
            if (UP_VALUE_UP.equals(providerInfo.getAttr(UP_KEY))) {
                upProviders.add(providerInfo);
            }
        }
        if (upProviders.isEmpty()) {

            SofaRpcRuntimeException exception = new SofaRpcRuntimeException("Provider 流量开关未打开, 请联系相关人员开启流量. InterfaceName: " + request.getInterfaceName());
            log.error(exception.getMessage(), exception);
            throw exception;
        } else {
            return upProviders;
        }
    }

    @Override
    public boolean needToLoad(ConsumerBootstrap consumerBootstrap) {
        ConsumerConfig consumerConfig = consumerBootstrap.getConsumerConfig();
        // 不是直连，且从注册中心订阅配置
        return StringUtils.isEmpty(consumerConfig.getDirectUrl()) && consumerConfig.isSubscribe();
    }
}
