#Use Official Python image as parent image

FROM python:3.8-slim-buster

# Set the working directory to /app

WORKDIR /app

# Copy the requirements.txt file into the working directory and install the dependencies

COPY requirements.txt .

RUN pip install -r requirements.txt

# Copy the rest of the files into the working directory

COPY . .

# Expose port 5000

EXPOSE 5000

# Set the environment variable FLASK_APP to app.py

ENV FLASK_APP=sentiment_data_puller.py

# Run the command "flask run" when the container launches

CMD ["flask", "run", "--host=0.0.0.0"]