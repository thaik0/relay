FROM alpine:3.22 AS build

RUN apk add --no-cache build-base cmake postgresql-dev

WORKDIR /source
COPY worker .

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel

FROM alpine:3.22

RUN apk add --no-cache libpq libstdc++

COPY --from=build /source/build/relay-worker /usr/local/bin/relay-worker

CMD ["relay-worker"]
