FROM python:3.12-slim

WORKDIR /app

# Instala dependências do sistema necessárias para psycopg (libpq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python primeiro (aproveita cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação
COPY app/ ./app/

# Roda como usuário não-root
RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 3000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000"]
