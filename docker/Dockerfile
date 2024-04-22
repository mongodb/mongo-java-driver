FROM python:3.8-slim

# Install curl
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install awscli and awscli-local
RUN pip install awscli awscli-local