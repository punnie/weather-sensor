FROM golang:1.17-buster AS build

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -o /weather-sensor


FROM gcr.io/distroless/base-debian10

WORKDIR /
COPY --from=build /weather-sensor /weather-sensor
USER nonroot:nonroot
ENTRYPOINT ["/weather-sensor"]
