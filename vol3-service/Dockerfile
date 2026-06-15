FROM golang:1.26-alpine AS go-builder
WORKDIR /build
COPY . .
RUN go.build -o vol3-app ./vol3-service/main.go

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y python3-dev && pip install volatility3
COPY --from=go-builder /build/vol3-app .
COPY ./vol3-service/ .
CMD ["./vol3-app"]