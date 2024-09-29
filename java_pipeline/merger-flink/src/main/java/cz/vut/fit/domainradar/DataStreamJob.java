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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.flink.models.BaseKafkaDomainResult;
import cz.vut.fit.domainradar.flink.models.KafkaDNSResult;
import cz.vut.fit.domainradar.models.results.AllCollectedData;
import cz.vut.fit.domainradar.models.results.DNSResult;
import cz.vut.fit.domainradar.models.results.Result;
import cz.vut.fit.domainradar.models.results.TLSResult;
import cz.vut.fit.domainradar.serialization.JsonDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;


public class DataStreamJob {

    private static final Properties kafkaProperties = new Properties();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.FORCE_AVRO, Boolean.TRUE);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(pipelineConfig);

        // Read configuration
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        ParameterTool params = ParameterTool.fromPropertiesFile("/home/ondryaso/Projects/domainradar/colext/java_pipeline/client.flink.properties");
        env.getConfig().setGlobalJobParameters(params);
        params.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                kafkaProperties.setProperty(k.substring(6), v);
            }
        });

        KafkaSource<KafkaDNSResult> dnsSource
                = KafkaSource.<KafkaDNSResult>builder()
                .setProperties(kafkaProperties)
                .setTopics("processed_DNS")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaDomainResultDeserializer<DNSResult, KafkaDNSResult>(
                        KafkaDNSResult.class.getName(), DNSResult.class.getName()
                ))
                .build();

        /*KafkaSource<ConsumedKafkaRecord<String, TLSResult>> tlsSource
                = makeKafkaSource("processed_TLS", new TypeHint<>() {
        });

        env.fromSource(dnsSource, makeWatermarkStrategy(), "Kafka: processed_DNS")
                .map(rec -> rec.getValue().toString())
                .print();
        */

        // Execute the program
        env.execute("DomainRadar Data Merger");
    }

    /*
    private static <K, V> KafkaSource<ConsumedKafkaRecord<K, V>> makeKafkaSource(
            final String topic, final TypeHint<ConsumedKafkaRecord<K, V>> typeHint) {
        return KafkaSource.<ConsumedKafkaRecord<K, V>>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaDeserializer<>(typeHint))
                .build();
    }*/

    private static <K, V> KafkaSink<Tuple2<K, V>> makeKafkaSink(final String topic) {
        return KafkaSink.<Tuple2<K, V>>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static <T extends BaseKafkaDomainResult> WatermarkStrategy<T> makeWatermarkStrategy() {
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, ts) -> event.getRecordMetadata().getCollectorTimestamp());
    }
}
