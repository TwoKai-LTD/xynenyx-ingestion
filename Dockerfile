# Build stage
FROM python:3.11-slim AS builder

WORKDIR /build

# Copy requirements file
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY app ./app
COPY scripts ./scripts

# Set ownership
RUN chown -R appuser:appuser /app

USER appuser

# Default entrypoint (can be overridden)
ENTRYPOINT ["python", "-m", "app.main"]

