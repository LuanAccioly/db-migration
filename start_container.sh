#!/bin/bash

# Verifica se o container natto-byte-api já existe
if docker ps -a --format '{{.Names}}' | grep -q "natto-byte-api"; then
    echo "Container natto-byte-api já existe. Parando e removendo..."
    
    # Para o container, caso esteja em execução
    docker stop natto-byte-api
    
    # Remove o container
    docker rm natto-byte-api
fi

# Executa o Docker container
docker run -d --name natto-byte-api -p "2712:2712" natto-byte

# Verifica se o container foi iniciado com sucesso
if [ $? -eq 0 ]; then
    echo "Container natto-byte-api iniciado com sucesso."
    exit 0  # Sucesso
else
    echo "Falha ao iniciar o container."
    exit 1  # Erro, o Airflow vai capturar isso
fi