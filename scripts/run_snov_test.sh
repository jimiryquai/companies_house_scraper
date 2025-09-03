#!/bin/bash
# Startup script for Snov.io testing

echo "=================================="
echo "Snov.io Integration Test Setup"
echo "=================================="

# Check if Flask is available
if ! python3 -c "import flask" 2>/dev/null; then
    echo "Installing Flask for webhook handler..."
    pip install flask
fi

# Check if we have the ngrok binary
if [ ! -f "./ngrok" ]; then
    echo "Error: ngrok binary not found!"
    echo "Please download ngrok first or make sure it's in the current directory"
    exit 1
fi

echo "Starting ngrok tunnel (this will run in background)..."
echo "Press Ctrl+C to stop everything"

# Start ngrok in background and capture the URL
./ngrok http 5000 &
NGROK_PID=$!

# Wait a moment for ngrok to start
sleep 3

# Get the ngrok URL
echo "Getting ngrok public URL..."
NGROK_URL=$(curl -s http://localhost:4040/api/tunnels | python3 -c "
import sys, json
data = json.load(sys.stdin)
for tunnel in data.get('tunnels', []):
    if tunnel['proto'] == 'https':
        print(tunnel['public_url'])
        break
")

if [ -z "$NGROK_URL" ]; then
    echo "Error: Could not get ngrok URL. Make sure ngrok is running."
    kill $NGROK_PID
    exit 1
fi

echo "Ngrok URL: $NGROK_URL"
echo "Webhook endpoint: $NGROK_URL/webhook"
echo ""

# Start the webhook handler in background
echo "Starting webhook handler..."
python3 test_webhook_handler.py &
WEBHOOK_PID=$!

# Wait for webhook handler to start
sleep 2

echo ""
echo "Setup complete!"
echo "=================================="
echo "Ngrok URL: $NGROK_URL"
echo "Webhook endpoint: $NGROK_URL/webhook"
echo "Stats endpoint: $NGROK_URL/stats"
echo "Health check: $NGROK_URL/health"
echo "=================================="
echo ""
echo "Now run the test script in another terminal:"
echo "python3 test_snov_250_companies.py"
echo ""
echo "Use this webhook URL when prompted: $NGROK_URL/webhook"
echo ""
echo "Press Ctrl+C to stop all services"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down services..."
    kill $NGROK_PID 2>/dev/null
    kill $WEBHOOK_PID 2>/dev/null
    echo "Services stopped."
    exit 0
}

# Set trap to cleanup on Ctrl+C
trap cleanup INT

# Wait indefinitely
while true; do
    sleep 1
done
