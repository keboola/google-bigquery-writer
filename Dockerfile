FROM python:3.12

# Copy
COPY . /home/
WORKDIR /home/

# Install packages
RUN pip install --no-cache-dir -r /home/requirements.txt

# download and add permission to run Split Table CLI
RUN apt-get update && apt-get install -y wget jq
RUN wget $(curl -s https://api.github.com/repos/keboola/processor-split-table/releases/tags/v3.0.0 | jq -r '.assets[] | select(.name == "cli_linux_amd64") | .browser_download_url')
RUN chmod +x cli_linux_amd64

# Run the application
CMD python -u ./main.py
