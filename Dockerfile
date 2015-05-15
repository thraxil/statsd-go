FROM golang
ADD . /go/src/github.com/thraxil/statsd-go
RUN go install github.com/thraxil/statsd-go
EXPOSE 8125/udp
ENTRYPOINT ["/go/bin/statsd-go"]
