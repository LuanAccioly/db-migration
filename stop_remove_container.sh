#!/bin/bash

# Para o container
docker stop natto-byte-api

# Remove o container
docker rm natto-byte-api

# Verifica se o container foi removido com sucesso
if [ $? -eq 0 ]; then
    echo "Container natto-byte-api parado e removido com sucesso."
else
    echo "Falha ao parar ou remover o container."
fi