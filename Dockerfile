FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && pip install -r requirements.txt

# This is critical: tell Cloud Run what port we use
ENV PORT=9000

# Install functions-framework
RUN pip install functions-framework

# Tell it which function to invoke
ENV FUNCTION_TARGET=handler

# Start the functions framework server
CMD ["functions-framework", "--target=handler","--source=server.py"]
