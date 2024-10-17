# A Dockerfile for building an image that runs a standalone Kafka Connect instance
# with the DomainRadar custom extensions included in its plugins.
# Author: Ondřej Ondryáš <xondry02@vut.cz>

ARG KAFKA_URL=https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz

# Use Eclipse Temurin 21 JDK image as the base
FROM eclipse-temurin:21-jdk-jammy AS build

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /src

# Copy just the POMs to resolve dependencies (using cache)
# TODO: in future, it should be possible to use COPY --parents pom.xml **/pom.xml .
COPY pom.xml ./
COPY standalone-collectors/pom.xml ./standalone-collectors/
COPY merger-flink/pom.xml ./merger-flink/
COPY common/pom.xml ./common/
COPY serialization/pom.xml ./serialization/
COPY connect/pom.xml ./connect/

# Resolve dependencies
RUN --mount=type=cache,target=/root/.m2/ mvn clean -pl connect -am && \
    mvn dependency:go-offline -DexcludeGroupIds=cz.vut.fit.domainradar -pl connect -am

# Copy the source files
COPY ./ .

# This builds the JAR and outputs the dependencies to the "libs/" directory
# The JAR is moved to the same directory; it can then be copied to the Kafka Connect plugins directory as a whole
RUN --mount=type=cache,target=/root/.m2/ --mount=type=cache,target=/src/target mvn package -pl "connect" -am && \
    cd "connect/target" && mv *.jar libs/

# Use the JRE variant for Kafka Connect runtime
FROM eclipse-temurin:21-jre AS runtime-connect
ARG KAFKA_URL

RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget -nv -O kafka.tgz "${KAFKA_URL}" && \
    mkdir /opt/kafka && \
    tar -xzf kafka.tgz -C /opt/kafka --strip-components=1 && \
    mkdir /opt/kafka-connect && \
    mkdir /opt/kafka-connect/plugins && \
    rm kafka.tgz && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /src/connect/target/libs /opt/kafka-connect/plugins/domainradar
CMD /opt/kafka/bin/connect-standalone.sh /opt/kafka-connect/config/*.properties
