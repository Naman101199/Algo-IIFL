#!/bin/bash

# Get public IP
export EC2_PUBLIC_IP=$(curl ifconfig.me)

# Install Java 1.8
sudo yum install -y java-1.8.0

# Install Docker
sudo dnf install -y docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install Development Tools and dependencies
sudo dnf install -y libxcrypt-compat
sudo dnf groupinstall "Development Tools" -y
sudo dnf install -y openssl-devel bzip2-devel libffi-devel

# Start and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add current user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Download and install Python 3.11.9
cd /usr/src
sudo wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
sudo tar xzf Python-3.11.9.tgz
cd Python-3.11.9
sudo ./configure --enable-optimizations
sudo make altinstall

# Verify Python installation
python3.11 --version