#!/bin/bash

#####################################
###### NAVID TAVAKOLI SHALMANI ######
#####################################


# Optional: activate venv to ensure `airflow` CLI is on PATH
[[ -d "$VENV_DIR" ]] && source "$VENV_DIR/bin/activate" || true

#One-off Command (or Foreground Process)
airflow db migrate

#Daemon Command (or Background Process)
airflow api-server --port 8080 > api-server.log 2>&1 &
#2>&1 redirect both stdout and stderr to spi-server.log file
# the sign "&" do Non-blocking CLI for us.

airflow scheduler > scheduler.log 2>&1 &
airflow triggerer > triggerer.log 2>&1 &
airflow dag-processor > dag-processor.log 2>&1 &


echo "To monitor logs, you can use the following commands:"

echo "  tail -f api-server.log"
echo "  tail -f scheduler.log"
echo "  tail -f dag-processor.log"
echo "  tail -f triggerer.log"

echo " ENJOY IT! 😎"
echo " Created by ***NAVID TAAVAKOLI SHALMANI*** "



