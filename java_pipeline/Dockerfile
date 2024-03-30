# Use Eclipse Temurin 21 JDK image as the base
FROM eclipse-temurin:21-jdk-jammy AS build

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /src

# Copy the project into the image
COPY ./ .
# Use Maven to compile and package the Java project
RUN mvn clean
RUN mvn package
RUN mv "$(echo 'target/${project.artifactId}-${project.version}-jar-with-dependencies.jar' | mvn -N -q -DforceStdout help:evaluate)" target.jar

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /src/target.jar ./domainradar-collector.jar

FROM runtime AS streams
WORKDIR /app
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.streams.StreamsPipelineRunner"]

FROM runtime AS standalone
WORKDIR /app
ENTRYPOINT ["java", "-cp", "/app/domainradar-collector.jar", "cz.vut.fit.domainradar.standalone.StandaloneCollectorRunner"]