# syntax=docker/dockerfile:1.7
# 로컬 Docker Compose용 빌드. 레지스트리에 올리는 멀티 아키텍처 이미지는 ko로 만든다 (.ko.yaml)
# 빌드는 항상 빌드 머신의 아키텍처에서 실행하고 Go 교차 컴파일로 대상 아키텍처 바이너리를 만든다.
# 대상 아키텍처 컨테이너에서 컴파일하면 QEMU 에뮬레이션 때문에 몇 배 느려진다.
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
COPY static ./static
COPY db ./db
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/demo-app .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/demo-app /demo-app
EXPOSE 8080
ENTRYPOINT ["/demo-app"]
