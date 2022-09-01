FROM golang:1.19 AS builder


# Set the Current Working Directory inside the container
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go test -v ./...

WORKDIR /app/cmd/integration-cip-skidspar

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

FROM registry.access.redhat.com/ubi8/ubi-minimal
WORKDIR /opt/diwise

LABEL "org.opencontainers.image.source" = "https://github.com/diwise/integration-cip-skidspar"

COPY --from=builder --chown=1001 /app/cmd/integration-cip-skidspar/integration-cip-skidspar /opt/diwise/

RUN chown -R 1001 /opt/diwise

USER 1001

ENTRYPOINT ["/opt/diwise/integration-cip-skidspar"]
