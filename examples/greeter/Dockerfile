FROM golang:1.14

RUN mkdir -p go/greeter

COPY . go/greeter

WORKDIR go/greeter

RUN go get -d -v ./...

RUN go build -v ./...

CMD ["./greeter"]