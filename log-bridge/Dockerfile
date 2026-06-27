FROM alpine:3.20 AS healer-build
RUN apk add --no-cache build-base cmake
WORKDIR /build
COPY self-healer/ ./
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

FROM python:3.12-slim
COPY --from=healer-build /build/build/self-healer /usr/local/bin/self-healer

WORKDIR /bridge
COPY log-bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY log-bridge/bridge.py .

CMD ["python", "bridge.py", "/logs/sample.txt"]
