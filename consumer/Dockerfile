FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install pandas kafka-python gspread oauth2client
CMD ["python", "consumer.py"]
