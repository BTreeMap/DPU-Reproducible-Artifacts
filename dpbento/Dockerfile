FROM ubuntu:noble AS builder

# Set non-interactive frontend for apt-get to avoid prompts
ARG DEBIAN_FRONTEND=noninteractive

# Install build tools, Python, and utilities
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        build-essential \
        cmake \
        curl \
        git \
        gpg \
        meson \
        ninja-build \
        openssl \
        python-is-python3 \
        python3-full \
        python3-pip \
        python3-setuptools \
        python3-venv \
        sshpass \
        sudo \
        unzip \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM scratch

# Set timezone
ENV TZ=America/Toronto

COPY --from=builder / /
