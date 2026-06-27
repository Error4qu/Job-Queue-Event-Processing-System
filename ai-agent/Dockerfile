FROM alpine:3.20 AS healer-build
RUN apk add --no-cache build-base cmake
WORKDIR /build
COPY self-healer/ ./
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

FROM python:3.12-slim

COPY --from=healer-build /build/build/self-healer /usr/local/bin/self-healer
COPY self-healer/examples/sample.txt /app/self-healer/examples/sample.txt

WORKDIR /app

COPY ai-agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ai-agent/app/ ./app/

ENV PYTHONUNBUFFERED=1
ENV SELF_HEALER_BIN=/usr/local/bin/self-healer
EXPOSE 8090

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
