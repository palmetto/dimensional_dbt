FROM python:3.8
COPY ./dimensional-dbt /dimensional-dbt
COPY ./test-dimensional-dbt /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD} 
RUN pip install -r requirements.txt