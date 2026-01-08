FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependencias del sistema:
# - LibreOffice (si lo ocupas)
# - Fonts
# - Tesseract (OCR)
# - ZBar (leer QR con pyzbar)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice \
    libreoffice-writer \
    fonts-dejavu-core \
    fontconfig \
    tesseract-ocr \
    tesseract-ocr-spa \
    libzbar0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD gunicorn rfc:app --bind 0.0.0.0:$PORT
