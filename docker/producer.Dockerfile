# Image du producer (S1) : Python + nos dépendances + notre code.
FROM python:3.11-slim

WORKDIR /app

# On copie d'abord requirements.txt seul : Docker garde cette étape en cache
# tant que le fichier ne change pas (build plus rapide).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY producer ./producer
COPY scripts ./scripts

CMD ["python", "-m", "producer.main"]