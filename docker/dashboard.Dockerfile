# Dashboard Streamlit (phase 3)
FROM python:3.11-slim

WORKDIR /app
COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt

# Le code est monté par docker-compose (./ -> /app) : pas besoin de le copier
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
