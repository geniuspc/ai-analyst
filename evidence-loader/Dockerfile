FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/evidence-loader ./cmd/evidence-loader/main.go


FROM alpine:3.20

RUN apk add --no-cache python3 ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/evidence-loader .
COPY vol.py .
RUN mkdir -p /evidence
EXPOSE 8080
CMD ["./evidence-loader"]