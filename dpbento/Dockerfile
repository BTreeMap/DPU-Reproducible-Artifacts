FROM ubuntu:noble AS builder

# Set non-interactive frontend for apt-get to avoid prompts
ARG DEBIAN_FRONTEND=noninteractive

COPY root/ /

RUN set -ex && \
    \
    # Install build tools, Python, and utilities
    apt-get update && \
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
    \
    # Unzip dpbento archive, remove zip, and rename directory for consistency
    unzip /opt/dpbento.zip -d /opt && \
    rm /opt/dpbento.zip && \
    mv /opt/dpBento /opt/dpbento && \
    \
    # Create and upgrade Python virtual environment
    python3 -m venv /opt/shared-venv --upgrade-deps

ENV PATH=/opt/shared-venv/bin:$PATH

WORKDIR /opt/dpbento

# Initialize and update git submodules
RUN git submodule update --init --recursive

FROM scratch

# Set timezone environment variable
ENV TZ=America/Toronto

COPY --from=builder / /

# Run dpbento using Python
ENTRYPOINT ["/usr/bin/env", "python3", "/opt/dpbento/run_dpbento.py"]
