FROM python:3.6

# Install packages
RUN pip install --no-cache-dir --ignore-installed \
		pytest \
        pytest-cov \
        google-cloud-bigquery==1.25.0 \
        oauth2client==4.1.3 \
        coverage==4.5.1 \
        flake8 \
        backoff

RUN pip install --upgrade --no-cache-dir --ignore-installed https://github.com/keboola/python-docker-application/archive/refs/tags/1.3.0.zip

RUN pip freeze

# Copy
COPY . /home/
WORKDIR /home/

# add permission to run Split Table CLI
RUN chmod +x cli_linux_amd64

# Run the application
CMD python -u ./main.py
