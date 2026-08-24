# Build image for scripts/sandbox-build.sh: JDK 25 + curl (TusStreamingMemoryTest shells out to curl).
FROM docker.io/eclipse-temurin:25-jdk
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
