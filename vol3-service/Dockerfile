FROM golang:1.26-alpine AS go-builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY vol3-service/ vol3-service/
RUN go build -o vol3-app ./vol3-service/cmd/main.go

FROM python:3.11-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir volatility3
WORKDIR /app
COPY --from=go-builder /app/vol3-app .
COPY ./vol3-service/vol.py .
COPY ./volatility3/volatility3/symbols/linux \
     /usr/local/lib/python3.11/site-packages/volatility3/symbols/linux
CMD ["./vol3-app"]