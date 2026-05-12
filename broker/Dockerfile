FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init broker
RUN go build -o broker

CMD ["./broker"]