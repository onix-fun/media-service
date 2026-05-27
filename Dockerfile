FROM alpine:latest

# Add ca-certificates and tzdata for HTTPS (MinIO/S3) and time functions
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app/

# Copy the pre-built binary
COPY bin/media-service-linux-amd64 ./media-service

# Copy migrations folder so auto-migrate works
COPY migrations ./migrations

# Expose port
EXPOSE 8080

# Command to run
CMD ["./media-service"]
