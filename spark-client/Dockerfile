FROM standalone-spark-spark-master

# Create a directory in home for mc
RUN mkdir -p /tmp/bin

# Download MinIO client there
RUN curl -o /tmp/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x /tmp/bin/mc

# Add the directory to PATH
ENV PATH="/tmp/bin:${PATH}"

# Configure MinIO alias
RUN mc alias set telco_minio http://minio:9000 telcoaz Telco12345 --api S3v4

# Expose Jupyter port
EXPOSE 8888

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]



