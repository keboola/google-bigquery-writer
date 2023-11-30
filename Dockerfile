FROM python:3.6

# Copy
COPY . /home/
WORKDIR /home/

# Install packages
RUN pip install --no-cache-dir -r /home/requirements.txt
RUN pip install --upgrade --no-cache-dir --ignore-installed https://github.com/keboola/python-docker-application/archive/refs/tags/1.3.0.zip

# add permission to run Split Table CLI
RUN chmod +x cli_linux_amd64

# Run the application
CMD python -u ./main.py
