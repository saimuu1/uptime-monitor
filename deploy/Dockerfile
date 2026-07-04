# ---- build stage: compile one service into a static binary ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Which cmd/<SERVICE> to build (scheduler, checker, evaluator, web, migrate...).
ARG SERVICE
RUN test -n "$SERVICE" # fail early if not provided
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${SERVICE}

# ---- final stage: tiny image with just the binary ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
