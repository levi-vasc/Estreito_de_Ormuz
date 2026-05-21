FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init sensor
RUN go build -o sensor

CMD ["./sensor"]