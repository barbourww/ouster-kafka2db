FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


RUN mkdir -p /var/log/ouster2kafka
CMD ["python3", "ouster2kafka.py"]