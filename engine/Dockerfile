# Build context: repository root (the build needs proto/).
#   docker build -f engine/Dockerfile --target test .     build and run C++ tests
#   docker build -f engine/Dockerfile -t clearline-engine . runtime image

FROM debian:trixie-slim AS build
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      g++ cmake make pkg-config \
      libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc libgtest-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto/ proto/
COPY engine/ engine/
RUN cmake -S engine -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build -j"$(nproc)"

FROM build AS test
RUN cd build && ctest --output-on-failure

FROM debian:trixie-slim AS runtime
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/matcher-server /usr/local/bin/matcher-server
USER nobody
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/matcher-server"]
CMD ["--addr", "0.0.0.0:50051"]
