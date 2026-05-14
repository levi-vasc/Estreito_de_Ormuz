FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init base
RUN go build -o base

CMD ["./base"]