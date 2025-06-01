FROM python:3.11
WORKDIR /app
COPY . .
COPY data /app/data
RUN pip install pandas faker kafka-python
CMD ["python", "producer.py"]
