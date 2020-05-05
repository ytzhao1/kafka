/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class DefaultPartitioner implements Partitioner {

    //counter 初始化为一个随机数。注意：是一个AtomicInteger
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
     * value.
     *
     * 确定性地将数字转换为正值的廉价方法。 当输入为
     * 正号，返回原始值。 输入数字为负数时，返回
     * 正值是原始值位，并且与0x7fffffff绝对不是绝对值
     * 值。
     *
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition.
     *
     * 注意：将来更改此方法可能会导致分区选择不正确
     * 与已经放置在分区上的现有消息兼容。
     *
     * @param number a given number
     * @return a positive number.
     */
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     * 根据给定的记录，计算分区
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //从Cluster中获取对应的topic的分区信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //分区的个数
        int numPartitions = partitions.size();
        //如果消息的key为null
        if (keyBytes == null) {
            //递增counter
            int nextValue = counter.getAndIncrement();
            //选择availablePartitions
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                //nextValue取绝对值，并对可用的分区数取余
                int part = DefaultPartitioner.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // 没有可用的分区，就提供一个不可用的分区，随机提供一个partitionId
                return DefaultPartitioner.toPositive(nextValue) % numPartitions;
            }
        } else {
            // 对key用murumur2算法进行hash，选择分区
            return DefaultPartitioner.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}

}
