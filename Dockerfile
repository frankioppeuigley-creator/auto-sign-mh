FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 禁用 Python 输出缓冲，确保日志实时显示
ENV PYTHONUNBUFFERED=1

CMD ["python", "run_proxy.py"]
