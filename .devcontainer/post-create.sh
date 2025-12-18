#!/bin/sh
#
# Post-create script for DBP ETL Dev Container
# Runs after the container is created to set up the development environment

set -e

# Configure git safe directory
git config --global --add safe.directory /app

# Create vscode home directory if it doesn't exist
mkdir -p /home/vscode

# Create necessary directories based on dbp-etl.cfg
echo "Creating required directories..."
mkdir -p /home/vscode/files/upload
mkdir -p /home/vscode/files/quarantine
mkdir -p /home/vscode/files/duplicate
mkdir -p /home/vscode/files/accepted
mkdir -p /home/vscode/files/errors
mkdir -p /home/vscode/files/bucket
touch /home/vscode/files/errors/AcceptErrors.txt

# Set proper ownership for directories
chown -R vscode:vscode /home/vscode/files

# Set up shell profile to auto-activate virtual environment
echo ". /app/venv/bin/activate" > /home/vscode/.profile
echo 'export PS1="(venv) \w \$ "' >> /home/vscode/.profile

# Activate virtual environment for this script
. /home/vscode/.profile

# Display welcome message and environment info
echo ""
echo "=== DBP ETL Development Container ==="
echo ""

echo "Python version:"
python3 --version
echo ""

echo "Virtual environment:"
which python3
echo ""

echo "Installed packages:"
pip list | grep -E "(boto3|pymysql|requests|awscli|pytz)"
echo ""

echo "Verifying boto3:"
python3 -c "import boto3; print('✓ boto3', boto3.__version__)"
echo ""

echo "=== Ready for development! ==="
echo ""
