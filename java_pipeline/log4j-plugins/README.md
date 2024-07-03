The `log4j-plugins` directory contains a patched version of the Kafka appender
from Log4J 2. It adds propagation of the LogEvent to the StrSubstitutor
replace operation which handles the `key` settings of the appender. This way,
the event-related lookups (such as ${event:Logger}) can be used to manipulate 
the key of events published to Kafka.

To use the plugin:
1. Uncomment the `<module>log4j-plugins</module>` line in the parent 
   [pom.xml](../pom.xml).
2. Add a dependency in the consuming projects:
   ```xml
   <dependency>
     <groupId>${project.groupId}</groupId>
     <artifactId>log4j-plugins</artifactId>
     <version>${project.version}</version>
   </dependency>
   ```
3. Use the `KafkaPatched` appender:
   ```xml
   <Appenders>
     <KafkaPatched name="Kafka" key="${event:Logger}">
       <Property name="bootstrap.servers">localhost:9092</Property>
     </KafkaPatched>
   </Appenders> 
   ```
