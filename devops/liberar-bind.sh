#!/bin/bash

# Este script serve para permitir que o executável do monitor possa ouvir na porta 443. É usado em produção.
# No Linux precisa ter permissão de root para ouvir em portas abaixo de 1000. Em produção o serviço roda pelo
# usuário "diel" e precisa ouvir na porta 443.


DIEL_HOME=${DIEL_HOME-$HOME}

if [ -e ./broker-connections-gateway ]; then
  sudo setcap cap_net_bind_service=+ep ./broker-connections-gateway
else
  sudo setcap cap_net_bind_service=+ep $DIEL_HOME/broker-connections-gateway/broker-connections-gateway
fi
