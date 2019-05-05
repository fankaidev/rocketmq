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
package org.apache.rocketmq.common;

import lombok.Data;
import org.apache.rocketmq.common.constant.PermName;

@Data
public class TopicConfig {
    private static final String SEPARATOR = " ";
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private String topicName;
    private int readQueueNums = defaultReadQueueNums;
    private int writeQueueNums = defaultWriteQueueNums;
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    private int topicSysFlag = 0;
    private boolean order = false;

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.perm);
        sb.append(SEPARATOR);
        sb.append(this.topicFilterType);

        return sb.toString();
    }

    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs != null && strs.length == 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            return true;
        }

        return false;
    }

}
