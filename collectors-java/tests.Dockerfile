FROM maven:3.9.9-eclipse-temurin-21

ARG UID=1000
ARG GID=1000

WORKDIR /work

COPY pom.xml /work/pom.xml
COPY common/pom.xml /work/common/pom.xml
COPY serialization/pom.xml /work/serialization/pom.xml
COPY connect/pom.xml /work/connect/pom.xml
COPY standalone-collectors/pom.xml /work/standalone-collectors/pom.xml

ENV MAVEN_ARGS="-Dmaven.repo.local=/opt/m2/repository"
RUN mkdir -p /opt/m2 && chown -R ${UID}:${GID} /opt/m2 /work

USER ${UID}:${GID}

RUN mvn -pl standalone-collectors -am -DskipTests install && \
    mvn -pl standalone-collectors -am -DskipTests dependency:go-offline

CMD ["mvn", "-pl", "standalone-collectors", "-am", "-Dgroups=integration", "-Dsurefire.failIfNoSpecifiedTests=false", "test", "-X"]
