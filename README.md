# DomainRadar Pipeline

This project contains applications that together make up the core process of DomainRadar:
- the **data collectors** fetch data on domain names from external sources (such as DNS),
- the **data merger** combines the data from all the collectors,
- the **feature extractor** computes the feature vector from the data,
- the **classifier pipeline component** fetches data from the extractor, passes them to the classifiers ([domainradar-clf](https://github.com/nesfit/domainradar-clf)) and stores the results.

See the [DomainRadar Pipeline & Models](https://github.com/nesfit/domainradar/blob/main/docs/pipeline_and_models.md) documentation for detailed information on the individual components, data flow between them and the used models.

## Architecture

The pipeline is constituted by a series of lightweight applications that perform a consume-process-produce cycle; the output of a certain collector is the input of another. This way, each pipeline component can be deployed separately and run in one or multiple instances to distribute the workload (limited by the number of partitions configured in Kafka for the source topics). At the end of the pipeline are the merger components that combine results from the collectors.

The components are implemented using several frameworks:

- Java / [Kafka Streams](https://kafka.apache.org/documentation/streams/):
    - [Data merger](java_pipeline/streams-components/src/main/java/cz/vut/fit/domainradar/streams/mergers/CollectedDataMergerComponent.java)
- Java / [Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer):
    - [TLS collector](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/collectors/TLSCollector.java)
    - [NERD collector](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/collectors/NERDCollector.java)
    - [GEO-ASN collector](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/collectors/GeoAsnCollector.java)
- Python / [Faust](https://faust-streaming.github.io/faust/):
    - [Zone collector](python_pipeline/collector/collectors/zone/zone.py)
    - [DNS collector](python_pipeline/collector/collectors/dns/dnscol.py)
    - [RDAP-DN collector](python_pipeline/collector/collectors/rdap_dn/rdap_dn.py)
    - [RDAP-IP collector](python_pipeline/collector/collectors/rdap_ip/rdap_ip.py)
    - [RTT collector](python_pipeline/collector/collectors/rtt/rtt.py)
    - [Extractor](python_pipeline/extractor/extractor)

## Running

### The Java-based collectors

The collectors based on Parallel Consumer are executed using a common [standalone runner](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/StandaloneCollectorRunner.java). Several different collectors may be started from a single runner instance. In this case, the collectors are totally independent: their consumer group ID is formed as "[provided app ID]-[collector identifier]". All instances of a given collector **must** be started with the same app ID; but it doesn't matter what collectors are started inside a single runner instance.


### The Faust-based collectors

The Faust-based components do not have a shared runner and each must be started separately. It still holds that the same app ID **must** be used for all running instances of a single component. Refer to the [python_pipeline/README.md](python_pipeline/README.md) file for more information.

### The data merger

The data merger is executed using the [Streams runner](java_pipeline/streams-components/src/main/java/cz/vut/fit/domainradar/streams/StreamsPipelineRunner.java). You **must** use the same app ID for all runner instances that contain this component. You can build and start the merger using the following commands:

```sh
cd java_pipeline
mvn package -pl streams-components -am
java -cp "streams-components/target/streams-components-1.0.0-SNAPSHOT-jar-with-dependencies.jar" "cz.vut.fit.domainradar.streams.StreamsPipelineRunner" --merger -id domainradar-merger -p "[client config file].properties" -s kafka:9092
# use -Dlog4j2.configurationFile=file:[config file].xml to override the logger configuration
```

