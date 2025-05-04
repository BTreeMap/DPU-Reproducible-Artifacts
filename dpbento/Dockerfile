FROM ubuntu:noble AS builder

# Set non-interactive frontend for apt-get to avoid prompts
ARG DEBIAN_FRONTEND=noninteractive

COPY root/ /

# Install build tools, Python, and utilities, then unzip and move dpbento
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        build-essential \
        cmake \
        curl \
        git \
        gnupg \
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
    rm -rf /var/lib/apt/lists/* && \
    unzip /opt/dpbento.zip && \
    rm /opt/dpbento.zip && \
    mv /opt/dpBento-main /opt/dpbento

FROM scratch

# Set timezone environment variable
ENV TZ=America/Toronto

COPY --from=builder / /

ENTRYPOINT ["/usr/bin/env", "python3", "/opt/dpbento/run_dpbento.py"]
