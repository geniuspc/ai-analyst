FROM golang:1.26-alpine AS builder

RUN apk add --no-no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o decision-engine ./cmd/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/decision-engine .
ENTRYPOINT ["./decision-engine"]