# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go.mod and go.sum and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# Final stage
FROM alpine:3.18

# Set working directory
WORKDIR /app

# Install required packages
RUN apk add --no-cache ca-certificates curl

# Copy binary from builder stage
COPY --from=builder /app/main .

# Create temporary directory for uploads
RUN mkdir -p /tmp/uploads

# Expose application port
EXPOSE 8080

# Set environment variables
ENV PORT=8080

# Run the application
CMD ["./main"]