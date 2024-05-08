# Use Eclipse Temurin 21 JDK image as the base
FROM eclipse-temurin:21-jdk-jammy AS build

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /src

# Copy just the POM to resolve dependencies
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2/ mvn clean && mvn dependency:resolve

# Copy the source files
COPY ./ .

# Build the JAR with dependencies
RUN --mount=type=cache,target=/root/.m2/ --mount=type=cache,target=/src/target mvn package && \
    OUTPUT_JAR="$(echo '${project.artifactId}-${project.version}-jar-with-dependencies.jar' | mvn -N -q -DforceStdout help:evaluate)" && \
    echo "$OUTPUT_JAR" > artifact_name.txt && \
    mv "target/$OUTPUT_JAR" target.jar

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /src/target.jar ./domainradar-collector.jar

FROM runtime AS streams
WORKDIR /app
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.streams.StreamsPipelineRunner"]

FROM runtime AS standalone
WORKDIR /app
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.standalone.StandaloneCollectorRunner"]
