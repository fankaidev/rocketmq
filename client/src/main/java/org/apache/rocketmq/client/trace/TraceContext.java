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
package org.apache.rocketmq.client.trace;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageClientIDSetter;

import java.util.List;

/**
 * The context of Trace
 */
@Data
public class TraceContext implements Comparable<TraceContext> {

    private TraceType traceType;
    private long timeStamp = System.currentTimeMillis();
    private String regionId = "";
    private String regionName = "";
    private String groupName = "";
    private int costTime = 0;
    private boolean success = true;
    private String requestId = MessageClientIDSetter.createUniqID();
    private int contextCode = 0;
    private List<TraceBean> traceBeans;

    @Override
    public int compareTo(TraceContext o) {
        return (int) (this.timeStamp - o.getTimeStamp());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(traceType).append("_").append(groupName)
            .append("_").append(regionId).append("_").append(success).append("_");
        if (traceBeans != null && traceBeans.size() > 0) {
            for (TraceBean bean : traceBeans) {
                sb.append(bean.getMsgId() + "_" + bean.getTopic() + "_");
            }
        }
        return "TraceContext{" + sb.toString() + '}';
    }
}
