FROM python:3.6

# Install packages
RUN pip install --no-cache-dir --ignore-installed \
		pytest \
        pytest-cov \
        google-cloud-bigquery==1.4.0 \
        oauth2client \
        coverage==4.3.4 \
        flake8

RUN pip install --upgrade --no-cache-dir --ignore-installed --cert=/tmp/cacert.pem git+git://github.com/keboola/python-docker-application.git@1.3.0

RUN pip freeze

# Copy
COPY . /home/
WORKDIR /home/

# Run the application
CMD python -u ./main.py