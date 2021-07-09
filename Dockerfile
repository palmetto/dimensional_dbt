FROM python:3.8
COPY . /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD} 
RUN pip install -r requirements.txt