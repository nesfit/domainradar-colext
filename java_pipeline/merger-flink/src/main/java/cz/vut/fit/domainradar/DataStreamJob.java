/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.vut.fit.domainradar;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.AllCollectedData;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.results.TLSResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.util.Properties;


public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read configuration
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStreamJob job = new DataStreamJob(env, params);
        job.build();

        // Execute the program
        env.execute("DomainRadar Data Merger");
    }

    private final Properties _kafkaProperties;
    private final StreamExecutionEnvironment _environment;
    private final ObjectMapper _mapper;

    private DataStreamJob(StreamExecutionEnvironment environment, ParameterTool parameters) {
        _kafkaProperties = new Properties();
        parameters.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                _kafkaProperties.setProperty(k.substring(6), v);
            }
        });

        _environment = environment;
        _mapper = Common.makeMapper().build();
    }

    private void build() {
        final StreamExecutionEnvironment env = _environment;

        KafkaSource<ConsumedKafkaRecord<String, DNSResult>> dnsSource = makeKafkaSource("processed_DNS");
        KafkaSource<ConsumedKafkaRecord<String, TLSResult>> tlsSource = makeKafkaSource("processed_TLS");
        KafkaSink<Tuple2<String, AllCollectedData>> sink = makeKafkaSink("all_collected_data");
    }

    private <K, V> KafkaSource<ConsumedKafkaRecord<K, V>> makeKafkaSource(final String topic) {
        return KafkaSource.<ConsumedKafkaRecord<K, V>>builder()
                .setProperties(_kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaDeserializer<>(_mapper))
                .build();
    }

    private <K, V> KafkaSink<Tuple2<K, V>> makeKafkaSink(final String topic) {
        return KafkaSink.<Tuple2<K, V>>builder()
                .setKafkaProducerConfig(_kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(_mapper, topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
