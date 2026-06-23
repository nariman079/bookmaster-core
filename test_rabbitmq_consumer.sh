#!/bin/bash

RABBITMQ_RUNNING=false

# Check if rabbitmq is running on port 5672
if ss -tlnp | grep -q 5672; then
  echo "RabbitMQ is already running."
  RABBITMQ_RUNNING=true
else
  echo "Starting RabbitMQ..."
  docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
  # Wait for rabbitmq to start
  sleep 10
fi

# Run the consumer in the background and capture output
cd /home/nariman079i/NotiesBooks/github/FIXMASTER
PYTHONPATH=/home/nariman079i/NotiesBooks/github/FIXMASTER:$PYTHONPATH python -m notification_service.notification > consumer.log 2>&1 &
CONSUMER_PID=$!
echo "Consumer started with PID $CONSUMER_PID"

# Wait for consumer to start
sleep 5

# Kill the consumer
kill $CONSUMER_PID
wait $CONSUMER_PID 2>/dev/null
echo "Consumer stopped."

# Check the log
if grep -q "RabbitMQ consumer started." consumer.log; then
  echo "SUCCESS: Consumer started successfully."
else
  echo "FAILURE: Consumer did not start successfully."
  cat consumer.log
fi

# If we started rabbitmq, stop it
if [ "$RABBITMQ_RUNNING" = false ]; then
  echo "Stopping RabbitMQ..."
  docker stop some-rabbit
  docker rm some-rabbit
fi