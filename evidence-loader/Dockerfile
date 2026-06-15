FROM golang:1.26-alpine
WORKDIR /app
COPY ../go.mod go.sum ./
RUN go.mod download
COPY .. .
RUN go build -o evidence-loader-app ./evidence-loader/main.go
CMD ["./app"]