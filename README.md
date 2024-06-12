# DomainRadar Pipeline

This project contains applications that together make up the core process of DomainRadar:
- the **data collectors** fetch data on domain names from external sources (such as DNS),
- the **mergers** combine data from various collectors,
- the **feature extractor** fetches the (semi-)integrated results from the mergers, and
- the **classifier pipeline component** fetches data from the extractor, passes them to the classifiers ([domainradar-clf](https://github.com/nesfit/domainradar-clf)) and stores the results.

See the [DomainRadar Pipeline & Models](https://github.com/nesfit/domainradar/blob/main/docs/pipeline_and_models.md) documentation for detailed information on the individual components, data flow between them and the used models.

## Architecture

The pipeline is constituted by a series of lightweight applications that perform a consume-process-produce cycle; the output of a certain collector is the input of another. This way, each pipeline component can be deployed separately and run in one or multiple instances to distribute the workload (limited by the number of partitions configured in Kafka for the source topics). At the end of the pipeline are the merger components that combine results from the collectors.

The components are implemented using several frameworks:

- Java / [Kafka Streams](https://kafka.apache.org/documentation/streams/):
    - [GeoIP collector](java_pipeline/streams-components/src/main/java/cz/vut/fit/domainradar/streams/collectors/GeoIPCollector.java)
    - [Data merger](java_pipeline/streams-components/src/main/java/cz/vut/fit/domainradar/streams/mergers/CollectedDataMergerComponent.java)
- Java / [Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer):
    - [TLS collector](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/collectors/TLSCollector.java)
    - [NERD collector](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/collectors/NERDCollector.java)
- Python / [Faust](https://faust-streaming.github.io/faust/):
    - [Zone collector](python_pipeline/collector/collectors/zone/zone.py)
    - [DNS collector](python_pipeline/collector/collectors/dns/dnscol.py)
    - [RDAP-DN collector](python_pipeline/collector/collectors/rdap_dn/rdap_dn.py)
    - [RDAP-IP collector](python_pipeline/collector/collectors/rdap_ip/rdap_ip.py)
    - [RTT collector](python_pipeline/collector/collectors/rtt/rtt.py)
    - [Extractor](python_pipeline/extractor/extractor)
    - [Classifier](python_pipeline/classifier/classifier_unit/app.py)

### Runners

The Kafka Streams based components are executed using a common [runner](java_pipeline/streams-components/src/main/java/cz/vut/fit/domainradar/streams/StreamsPipelineRunner.java). It builds a single Streams topology using the components requested by command-line arguments. Several components may be executed as a part of a single Streams app (under a single app ID). 

For each Streams-based component, you **must** use the same app ID for all runner instances that contain this component. You **must not** run a component in multiple different topology configurations, e.g. by running two instances with different IDs, one including the GeoIP collector, the other including the GeoIP collector and the mergers. You also **must not** share an app ID between Streams instances with varying topology configurations (i.e. enabled components). 

The ParallelConsumer-based components are also executed using a common [runner](java_pipeline/standalone-collectors/src/main/java/cz/vut/fit/domainradar/standalone/StandaloneCollectorRunner.java). Several different components may be started from a single runner instance. In this case, the components are totally independent: their consumer group ID is formed as "[provided app ID]-[component name]". Therefore, all instances of a given component **must** be started with the same app ID; but it doesn't matter what components are started inside a single runner instance.

The Faust-based components do not have a shared runner and each must be started separately. It still holds that the same app ID **must** be used for all running instances of a single component.
