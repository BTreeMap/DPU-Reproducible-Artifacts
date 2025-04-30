# Hephaestus

This repository contains reproducible artifacts for evaluating Data Processing Unit (DPU) performance and capabilities. **All artifacts are packaged as Docker images and uploaded to the GitHub Container Registry (GHCR) with tags like `ghcr.io/btreemap/hephaestus:<experiment-name>`**. Users are encouraged to run these pre-built images instead of building them locally to ensure consistent results.

## Purpose

The main objective of this repository is to provide standardized, reproducible experiments for evaluating Data Processing Units (DPUs) across various workloads and configurations. These artifacts enable consistent benchmarking and validation of DPU performance claims.

## Getting Started

### Prerequisites

- Docker installed on your machine
- Basic knowledge of containerization
- Access to DPU hardware (for hardware-specific tests)

### Using the Pre-built Images

**All experiment images are built and uploaded to the GitHub Container Registry (GHCR) with tags like `ghcr.io/btreemap/hephaestus:<experiment-name>`**.

You can pull and run the images directly:

```bash
docker run ghcr.io/btreemap/hephaestus:<experiment-name>
```

For example, to run the network offload benchmark:

```bash
docker run ghcr.io/btreemap/hephaestus:network-offload-benchmark
```

### Using Docker Compose

You can also use the images in your `docker-compose.yml` file:

```yaml
services:
  dpu-benchmark:
    image: ghcr.io/btreemap/hephaestus:network-offload-benchmark
    # Additional configuration as needed
```

## Available Artifacts

This repository includes reproducible artifacts for the following DPU experiments:

- **Network Offload Benchmarks**: Evaluate network processing performance when offloaded to DPUs
- **Security Function Benchmarks**: Measure performance of security functions (encryption, firewall, etc.)
- **Storage Acceleration Tests**: Evaluate NVMe-oF and storage processing performance
- **CPU Offload Measurements**: Quantify host CPU savings from DPU offloading

Each artifact contains detailed documentation on its methodology, expected results, and configuration parameters.

## Reproducing Results

For consistent results:

1. Use the pre-built images with the exact version tags
2. Follow the hardware setup instructions included in each artifact
3. Run experiments with the provided scripts to ensure methodology consistency
4. Compare your results with the reference results included in each artifact

## Contributing

Contributions that improve reproducibility or add new experiments are welcome. Please ensure all contributions maintain the rigorous standards for reproducibility.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

- Thanks to all researchers and engineers who contributed benchmark methodologies
- Special appreciation to the open-source community for providing tools that made these artifacts possible
