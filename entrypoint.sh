#!/bin/bash

# Verifica o primeiro argumento e escolhe qual script Python executar
if [ "$1" == "api" ]; then
    echo "Iniciando API..."
    exec python app/api.py
elif [ "$1" == "migrate" ]; then
    echo "Iniciando migração de dados..."
    exec python app/main.py
else
    echo "Argumento inválido. Use 'api' para iniciar a API ou 'migrate' para migração."
    exit 1
fi