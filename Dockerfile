# Use uma imagem base oficial do Python
FROM python:3.12-slim

# Instale curl, gpg e outras dependências necessárias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gpg \
        apt-transport-https \
        && rm -rf /var/lib/apt/lists/*

# Adicione a chave e o repositório do Microsoft SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc 
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg 
RUN curl https://packages.microsoft.com/config/debian/9/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN curl https://packages.microsoft.com/config/debian/10/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN apt-get install -y unixodbc-dev
RUN apt-get install -y libgssapi-krb5-2



# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos de requisitos e o código da aplicação
COPY requirements.txt ./
COPY .env ./
COPY . .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Configurar variáveis de ambiente no container
ENV PYTHONUNBUFFERED=1

# A aplicação não expõe portas porque não serve conteúdo via HTTP
# Mas é importante garantir que ela execute corretamente quando for chamada

# O ponto de entrada do container para rodar a aplicação
CMD ["python", "app/main.py"]