FROM ubuntu:noble

# Avoid interactive prompts during package installation
ARG DEBIAN_FRONTEND=noninteractive

# Install necessary tools and libraries
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
