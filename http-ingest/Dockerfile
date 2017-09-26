FROM openjdk:8-jre

WORKDIR /app
VOLUME ["/app/log"]
ENTRYPOINT ["/app/bin/http-ingest", "server", "config.yml"]
EXPOSE 9099 9099

COPY build/lib /app/lib
COPY build/scripts/http-ingest /app/bin/
COPY dist/config.yml /app

