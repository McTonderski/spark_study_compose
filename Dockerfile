# Use an official Spark base image
FROM bitnami/spark:latest

# Copy the Spark application code into the Docker image
COPY lab3.scala /opt/bitnami/spark/work-dir/

# Set the working directory
WORKDIR /opt/bitnami/spark/work-dir/

# Run the Spark Shell with the application code
CMD ["spark-shell", "-i", "lab3.scala"]
