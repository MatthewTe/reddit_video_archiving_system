FROM golang:1.23.1

WORKDIR /app

COPY . .

WORKDIR /app/rest_api

RUN go mod download && go build -o /build

EXPOSE 8080

CMD ["/build", "/app/.env"]