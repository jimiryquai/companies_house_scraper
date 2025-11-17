#!/bin/bash
# Railway Deployment Script
# This script deploys the Companies House streaming service to Railway

set -e

echo "=== Railway Deployment Script ==="
echo

# Check if logged in
if ! railway whoami > /dev/null 2>&1; then
    echo "❌ Not logged in to Railway"
    echo "Please run: railway login"
    echo "Then run this script again"
    exit 1
fi

echo "✅ Logged in to Railway as: $(railway whoami)"
echo

# Check if in a project
if railway status > /dev/null 2>&1; then
    echo "✅ Already in a Railway project"
    railway status
else
    echo "Creating new Railway project..."
    railway init
fi

echo
echo "Adding PostgreSQL database..."
railway add --database postgres

echo
echo "Setting environment variables..."
read -p "Enter CH_STREAMING_API_KEY: " streaming_key
read -p "Enter CH_COMPANIES_API_KEY: " companies_key
read -p "Enter CH_OFFICERS_API_KEY: " officers_key

railway variables set CH_STREAMING_API_KEY="$streaming_key"
railway variables set CH_COMPANIES_API_KEY="$companies_key"
railway variables set CH_OFFICERS_API_KEY="$officers_key"
railway variables set LOG_LEVEL="INFO"

echo
echo "✅ Environment variables set"
echo

echo "Deploying to Railway..."
railway up

echo
echo "=== ✅ Deployment Complete! ==="
echo
echo "View your deployment:"
echo "  railway open"
echo
echo "View logs:"
echo "  railway logs"
echo
echo "Get DATABASE_URL for n8n:"
echo "  railway variables"
echo
