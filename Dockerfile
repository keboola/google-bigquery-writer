FROM python:3.6

# Install packages
RUN pip install --no-cache-dir --ignore-installed \
		pytest \
        pytest-cov \
        google-cloud-bigquery==v3.11.4 \
        oauth2client \
        coverage==4.5.1 \
        flake8

RUN pip install --upgrade --no-cache-dir --ignore-installed https://github.com/keboola/python-docker-application/archive/refs/tags/1.3.0.zip

RUN pip freeze

# Copy
COPY . /home/
WORKDIR /home/

# Run the application
CMD python -u ./main.py
