FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init drone
RUN go build -o drone

CMD ["./drone"]