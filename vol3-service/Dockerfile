FROM golang:1.26-alpine AS go-builder
WORKDIR /app
COPY . .
RUN go.build -o vol3-app ./vol3-service/main.go

FROM python:3.11-slim
RUN apt-get update && apt-get install -y python3-dev git && \
    pip install volatility3
WORKDIR /app
COPY --from=go-builder /app/vol3-app .
COPY ./vol3-service/vol.py .
CMD ["./vol3-app"]