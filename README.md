# broker-connections-gateway
Monitora as conexões ao broker

Por padrão o Linux limita a quantidade de arquivos que um usuário pode manter abertos por segurança.
Sockets TCP são considerados arquivos pelo Linux.
Para cada dispositivo conectado este serviço mantém 2 sockets abertos, um com o dispositivo e outro com o broker.
No caso de processor normais (executados em um terminal) este limite pode ser cofnigurado da seguinte maneira:

At the end of the file /etc/security/limits.conf you need to add the following lines:
```
* soft nofile 64000
* hard nofile 64000
```

In the current console from root (sudo does not work) to do:
```
ulimit -n 64000
```

No caso de serviços executados pelo systemd, parece que esta configuração acima não é aplicada, tem que ser configurado dentro do arquivo .service pela propriedade `LimitNOFILE=64000`.
