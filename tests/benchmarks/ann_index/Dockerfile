FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-h5py \
        python3-numpy \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY harness /harness
COPY presets /presets

ENTRYPOINT ["python3", "/harness/bench.py"]
