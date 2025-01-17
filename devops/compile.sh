#!/bin/bash
RUSTFLAGS=-Awarnings cargo build --release || exit 1

./target/release/broker-connections-gateway --test-config ./configfile_example.json5 || exit 1
./target/release/broker-connections-gateway --test-config

mv broker-connections-gateway broker-connections-gateway-`date '+%FT%T'`
cp ./target/release/broker-connections-gateway .
cp -f ./target/release/broker-devel-gateway .

echo sudo setcap 'cap_net_bind_service=+ep' ./broker-connections-gateway
# sudo bash /root/liberar-portas.sh
