FROM python:3.10-slim

WORKDIR /app

# OpenCV + OpenMP runtime deps / Thư viện runtime OpenCV và OpenMP
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Bake paddlepaddle into image (paddleocr alone is not enough) /
# Cài paddlepaddle vào image (chỉ paddleocr thì chưa đủ)
RUN pip install --no-cache-dir "paddlepaddle==3.2.2" \
    && pip install --no-cache-dir -r requirements.txt

COPY app.py block_merge.py text_layout.py .

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
