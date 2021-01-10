FROM python:3.9.0

WORKDIR /mariaFlix

COPY requirements.txt .
COPY model.py .
COPY API.py .

RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["python", "API.py"]