FROM python:3.13-slim

RUN useradd --create-home appuser

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the application code needed to run
COPY api/ api/
COPY lib/ lib/
COPY index.py dev.py ./

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Development server with hot reload
CMD ["python", "dev.py"]
