FROM alpine:3.20 AS builder
RUN apk add --no-cache cmake g++ make
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /src/build/self-healer /app/self-healer
ENTRYPOINT ["/app/self-healer"]
