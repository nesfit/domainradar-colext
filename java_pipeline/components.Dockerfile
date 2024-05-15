# Use Eclipse Temurin 21 JDK image as the base for the building process
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
COPY streams-components/pom.xml ./streams-components/
COPY standalone-collectors/pom.xml ./standalone-collectors/
COPY common/pom.xml ./common/
COPY serialization/pom.xml ./serialization/
COPY connect/pom.xml ./connect/

# Resolve dependencies
RUN --mount=type=cache,target=/root/.m2/ mvn clean -pl '-:connect' && \
    mvn dependency:go-offline -DexcludeGroupIds=cz.vut.fit.domainradar -pl '-:connect'

# Copy the source files
COPY ./ .

FROM build AS build_component
ARG TARGET_PKG=streams-components
ENV TARGET_PKG=${TARGET_PKG}

# Build the JAR with dependencies
# Only TARGET_PKG will be built
RUN --mount=type=cache,target=/root/.m2/ --mount=type=cache,target=/src/target mvn package -pl ${TARGET_PKG} -am && \
    cd "$TARGET_PKG" && \
    OUTPUT_JAR="$TARGET_PKG/target/$(echo '${project.artifactId}-${project.version}-jar-with-dependencies.jar' | mvn -N -q -DforceStdout help:evaluate)" && \
    cd .. && \
    echo "$OUTPUT_JAR" > artifact_name.txt && \
    cp "$OUTPUT_JAR" target.jar

# Use the JRE variant for runtime
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build_component /src/target.jar ./domainradar-collector.jar
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar"]

FROM runtime AS runtime-streams
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.streams.StreamsPipelineRunner"]

FROM runtime AS runtime-standalone
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.standalone.StandaloneCollectorRunner"]
