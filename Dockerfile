FROM node:24-bookworm-slim AS web

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /web

ARG LYRA_WEB_GIT_HASH
RUN printf '%s\n' "$LYRA_WEB_GIT_HASH" | grep -Eq '^[0-9a-f]{40}$' \
    && git init . \
    && git remote add origin https://github.com/lyra-org/lyra-web.git \
    && git fetch --depth 1 origin "$LYRA_WEB_GIT_HASH" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "$LYRA_WEB_GIT_HASH"

RUN npm install --global "$(node -p 'require("./package.json").packageManager')" \
    && pnpm install --frozen-lockfile \
    && pnpm run check \
    && pnpm run build

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

RUN git clone --depth 1 --branch n8.0 https://github.com/FFmpeg/FFmpeg.git /ffmpeg-src

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

FROM debian:trixie-slim AS build-env

COPY --from=ffmpeg /usr/local/include/ /usr/local/include/
COPY --from=ffmpeg /usr/local/lib/ /usr/local/lib/
RUN ldconfig

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    pkg-config \
    clang \
    git \
    ca-certificates \
    libmp3lame-dev \
    libopus-dev \
    libvorbis-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
ENV PATH="/root/.cargo/bin:${PATH}"

ARG LYRA_GIT_HASH=unknown
ENV LYRA_GIT_HASH=${LYRA_GIT_HASH}
ARG LYRA_RELEASE_TAG=
ENV LYRA_RELEASE_TAG=${LYRA_RELEASE_TAG}

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY harmony-core/Cargo.toml harmony-core/Cargo.toml
COPY harmony-crypt/Cargo.toml harmony-crypt/Cargo.toml
COPY harmony-globals/Cargo.toml harmony-globals/Cargo.toml
COPY harmony-http/Cargo.toml harmony-http/Cargo.toml
COPY harmony-serde/Cargo.toml harmony-serde/Cargo.toml
COPY harmony-luau/Cargo.toml harmony-luau/Cargo.toml
COPY harmony-macros/Cargo.toml harmony-macros/Cargo.toml
COPY harmony-net/Cargo.toml harmony-net/Cargo.toml
COPY harmony-repository/Cargo.toml harmony-repository/Cargo.toml
COPY harmony-task/Cargo.toml harmony-task/Cargo.toml
COPY lyra-chromaprint/Cargo.toml lyra-chromaprint/Cargo.toml
COPY lyra-ffmpeg/Cargo.toml lyra-ffmpeg/Cargo.toml
COPY lyra-docs/Cargo.toml lyra-docs/Cargo.toml
COPY lyra-metadata/Cargo.toml lyra-metadata/Cargo.toml
COPY lyra-server/Cargo.toml lyra-server/Cargo.toml
COPY lyra-harmony-test/Cargo.toml lyra-harmony-test/Cargo.toml

# Stub source files for dependency caching layer.
RUN for dir in harmony-core harmony-crypt harmony-globals harmony-http harmony-serde harmony-luau harmony-macros harmony-net harmony-repository harmony-task lyra-chromaprint lyra-ffmpeg lyra-metadata lyra-harmony-test; do \
      mkdir -p "$dir/src" && echo '' > "$dir/src/lib.rs"; \
    done && \
    mkdir -p lyra-docs/src && echo 'fn main() {}' > lyra-docs/src/main.rs && \
    mkdir -p lyra-server/src && echo 'fn main() {}' > lyra-server/src/main.rs && echo '' > lyra-server/src/lib.rs

# Fetch all dependencies (fails fast on resolution/network errors).
RUN cargo fetch --locked

FROM build-env AS clippy

COPY . .
RUN cargo clippy --locked --workspace --all-targets -- -D warnings

FROM build-env AS builder

# Pre-compile dependencies. Stubs cause final link to fail — that's expected.
RUN cargo build --release -p lyra-server || true

COPY . .

RUN find harmony-* lyra-* -name '*.rs' -exec touch {} +

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

RUN groupadd -g 1000 lyra \
    && useradd -u 1000 -g 1000 -M -s /usr/sbin/nologin lyra

WORKDIR /

COPY --from=builder /build/target/release/lyra /usr/local/bin/lyra
COPY --from=web /web/dist/ /usr/share/lyra/web/

ARG LYRA_GIT_HASH=unknown
ARG LYRA_WEB_GIT_HASH
LABEL org.opencontainers.image.revision=${LYRA_GIT_HASH} \
    pub.lyra.web.revision=${LYRA_WEB_GIT_HASH}

RUN mkdir -p /data /plugins \
    && chown lyra:lyra /data /plugins

USER lyra

ENV LYRA_DATA_DIR=/data \
    LYRA_PLUGINS_DIR=/plugins \
    LYRA_STATIC_DIR=/usr/share/lyra/web

VOLUME ["/data", "/plugins"]

EXPOSE 4746

ENTRYPOINT ["lyra"]
CMD ["serve"]
