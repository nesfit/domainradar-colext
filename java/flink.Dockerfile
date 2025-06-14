# A Dockerfile for building the Flink-based merger and producing a Flink-derived image
# that runs it in the standalone mode.
# Author: Ondřej Ondryáš <xondry02@vut.cz>

# Use Eclipse Temurin 21 JDK image as the base for the building process
FROM docker.io/library/eclipse-temurin:21-jdk-jammy AS build

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
RUN --mount=type=cache,target=/root/.m2/ mvn clean -pl 'merger-flink' -am && \
    mvn dependency:go-offline -DexcludeGroupIds=cz.vut.fit.domainradar -pl 'merger-flink' -am

# Copy the source files
COPY ./ .

FROM build AS build_component
ARG TARGET_PKG=merger-flink
ENV TARGET_PKG=${TARGET_PKG}

# Build the JAR with dependencies
# Only TARGET_PKG will be built
RUN --mount=type=cache,target=/root/.m2/ --mount=type=cache,target=/src/target mvn package -pl ${TARGET_PKG} -am && \
    cd "$TARGET_PKG" && \
    OUTPUT_JAR="$TARGET_PKG/target/$(echo '${project.artifactId}-${project.version}.jar' | mvn -N -q -DforceStdout help:evaluate)" && \
    cd .. && \
    echo "$OUTPUT_JAR" > artifact_name.txt && \
    cp "$OUTPUT_JAR" target.jar

# Use the Flink image for runtime
FROM docker.io/library/flink:1.20.1-scala_2.12-java17 AS runtime
COPY --from=build_component /src/target.jar /opt/flink/usrlib/merger-flink.jar
RUN mkdir /flink-data && chown 9999:9999 /flink-data