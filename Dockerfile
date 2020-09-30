FROM golang:1.15-alpine as build
LABEL stage=intermediate

RUN apk add --update --no-cache git ca-certificates tzdata && update-ca-certificates
RUN adduser -D -g '' appuser

# Allow using a proxy
ARG GOPROXY

# ---------

WORKDIR /app-build

# Copy dependencies
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy code and build
COPY . .

# Ensure tests are green
RUN CGO_ENABLED=0 go test ./...

# Final build
RUN CGO_ENABLED=0 go build -o app -ldflags="-w -s" ./cmds/rebalance

# --------------------------------
FROM scratch
ENTRYPOINT ["./app"]

COPY --from=build /etc/passwd /etc/passwd
USER appuser

COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /etc/ssl/certs/ /etc/ssl/certs/
COPY --from=build /app-build/app app
