#!/bin/bash

# Executa o Docker container
docker run -d --name natto-byte-api -p "2712:2712" natto-byte api

# Verifica se o container foi iniciado com sucesso
if [ $? -eq 0 ]; then
    echo "Container natto-byte-api iniciado com sucesso."
else
    echo "Falha ao iniciar o container."
fi