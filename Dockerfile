FROM python:3.8
COPY ./macros /dimensional_dbt/macros
COPY ./dbt_project.yml /dimensional_dbt/dbt_project.yml
COPY ./test_dimensional_dbt /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD} 
RUN pip install -r requirements.txt