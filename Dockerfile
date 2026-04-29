FROM debian:trixie-slim AS ffmpeg

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    nasm \
    pkg-config \
    git \
    ca-certificates \
    libmp3lame-dev \
    libopus-dev \
    libvorbis-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch n8.0 https://git.ffmpeg.org/ffmpeg.git /ffmpeg-src

WORKDIR /ffmpeg-src
RUN ./configure \
    --prefix=/usr/local \
    --enable-shared \
    --disable-static \
    --disable-programs \
    --disable-doc \
    --enable-libmp3lame \
    --enable-libopus \
    --enable-libvorbis \
    && make -j"$(nproc)" \
    && make install

FROM debian:trixie-slim AS builder

COPY --from=ffmpeg /usr/local/include/ /usr/local/include/
COPY --from=ffmpeg /usr/local/lib/ /usr/local/lib/
RUN ldconfig

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    pkg-config \
    clang \
    git \
    ca-certificates \
    libmp3lame-dev \
    libopus-dev \
    libvorbis-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly --component rustfmt
ENV PATH="/root/.cargo/bin:${PATH}"

ARG LYRA_GIT_HASH=unknown
ENV LYRA_GIT_HASH=${LYRA_GIT_HASH}

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY harmony-core/Cargo.toml harmony-core/Cargo.toml
COPY harmony-crypt/Cargo.toml harmony-crypt/Cargo.toml
COPY harmony-globals/Cargo.toml harmony-globals/Cargo.toml
COPY harmony-http/Cargo.toml harmony-http/Cargo.toml
COPY harmony-json/Cargo.toml harmony-json/Cargo.toml
COPY harmony-luau/Cargo.toml harmony-luau/Cargo.toml
COPY harmony-macros/Cargo.toml harmony-macros/Cargo.toml
COPY harmony-net/Cargo.toml harmony-net/Cargo.toml
COPY harmony-task/Cargo.toml harmony-task/Cargo.toml
COPY lyra-chromaprint/Cargo.toml lyra-chromaprint/Cargo.toml
COPY lyra-ffmpeg/Cargo.toml lyra-ffmpeg/Cargo.toml
COPY lyra-metadata/Cargo.toml lyra-metadata/Cargo.toml
COPY lyra-server/Cargo.toml lyra-server/Cargo.toml
COPY lyra-harmony-test/Cargo.toml lyra-harmony-test/Cargo.toml

# Stub source files for dependency caching layer.
RUN for dir in harmony-core harmony-crypt harmony-globals harmony-http harmony-json harmony-luau harmony-net harmony-task lyra-chromaprint lyra-ffmpeg lyra-metadata lyra-harmony-test; do \
      mkdir -p "$dir/src" && echo '' > "$dir/src/lib.rs"; \
    done && \
    mkdir -p harmony-macros/src && echo '' > harmony-macros/src/lib.rs && \
    mkdir -p lyra-server/src && echo 'fn main() {}' > lyra-server/src/main.rs && echo '' > lyra-server/src/lib.rs

# Fetch all dependencies (fails fast on resolution/network errors).
RUN cargo fetch --locked

# Pre-compile dependencies. Stubs cause final link to fail — that's expected.
RUN cargo build --release -p lyra-server || true

COPY . .

RUN cargo build --release --locked -p lyra-server

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libmp3lame0 \
    libopus0 \
    libvorbis0a \
    libvorbisenc2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ffmpeg /usr/local/lib/lib*.so* /usr/local/lib/
RUN ldconfig

RUN useradd -r -s /bin/false lyra

WORKDIR /lyra

COPY --from=builder /build/target/release/lyra /usr/local/bin/lyra
COPY --from=builder /build/plugins/ /lyra/plugins/

RUN chown -R lyra:lyra /lyra

USER lyra

ENV LYRA_CONFIG_PATH=/lyra/config.json \
    LYRA_PLUGINS_DIR=/lyra/plugins

VOLUME ["/lyra/plugins"]

EXPOSE 4746

ENTRYPOINT ["lyra"]
CMD ["serve"]
