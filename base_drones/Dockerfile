FROM golang:1.22

WORKDIR /app

COPY . .

RUN go mod init base_drones
RUN go build -o base_drones

CMD ["./base_drones"]