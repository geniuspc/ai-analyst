FROM golang:1.26-alpine
WORKDIR /app
COPY ../go.mod go.sum ./
RUN go.mod download
COPY .. .
RUN go build -o decision_engine-app ./decision_engine/main.go
CMD ["./app"]