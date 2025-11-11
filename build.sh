#!/bin/bash

echo "Generating Swagger documentation..."
swag init -g cmd/main.go -o docs/

echo "Building application..."
go build -o bin/leb-control-api cmd/main.go

echo "Build completed. Binary available at: bin/leb-control-api"