FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init sensores
RUN go build -o sensores

CMD ["./sensores"]