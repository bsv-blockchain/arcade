#!/bin/bash
set -e

SWAG="${GOBIN:-$(go env GOPATH)/bin}/swag"

if ! command -v swag &> /dev/null && [ ! -f "$SWAG" ]; then
    echo "Installing swag..."
    go install github.com/swaggo/swag/cmd/swag@latest
fi

"$SWAG" init -g cmd/arcade/main.go -o docs --parseDependency
