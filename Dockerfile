FROM python:3.8
COPY ./macros /dimensional-dbt/macros
COPY ./dbt_project.yml /dimensional-dbt/dbt_project.yml
COPY ./test-dimensional-dbt /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD} 
RUN pip install -r requirements.txt