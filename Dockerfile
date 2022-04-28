FROM python:3

ENV PIP_EXTRA_INDEX_URL="https://artifactory.genmills.com/artifactory/api/pypi/python-release-local/simple" \
    PIP_TRUSTED_HOST=artifactory.genmills.com

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY src/bigquery_ddl_deploy.py /bigquery_ddl_deploy.py

# Executes `publish_module.py` when the Docker container starts up
CMD ["python", "/bigquery_ddl_deploy.py"]