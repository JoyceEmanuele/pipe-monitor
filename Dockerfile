# Stage 1: Compilar o projeto Rust
FROM rust:1.73.0 as builder

WORKDIR /app

# Instalar dependências necessárias
RUN apt-get update && \
    apt-get install -y build-essential libssl-dev && \
    cd /tmp && \
    wget https://github.com/Kitware/CMake/releases/download/v3.20.0/cmake-3.20.0.tar.gz && \
    tar -zxvf cmake-3.20.0.tar.gz && \
    cd cmake-3.20.0 && \
    ./bootstrap && \
    make && \
    make install && \
    export CARGO_NET_GIT_FETCH_WITH_CLI=true && \
    apt-get install -y librust-openssl-dev default-libmysqlclient-dev

# Copiar o código-fonte restante e compilar
COPY . .
RUN cargo build --release

# Stage 2: Criar a imagem final
FROM ubuntu:jammy

WORKDIR /app

# Copiar o binário compilado da stage anterior
COPY --from=builder /app/target/release/broker-connections-gateway .
COPY --from=builder /app/configfile.json5 .

RUN apt-get update \
    && apt-get install -y libcap2-bin \
    && rm -rf /var/lib/apt/lists/*

RUN setcap 'cap_net_bind_service=+ep' /app/broker-connections-gateway

EXPOSE 46882 8883

# Executar o binário
CMD ["./broker-connections-gateway"]
