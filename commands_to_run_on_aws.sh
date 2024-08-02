sudo dnf update -y
sudo dnf install -y docker
sudo dnf install -y libxcrypt-compat
sudo dnf install -y git
sudo dnf groupinstall "Development Tools" -y
sudo dnf install openssl-devel bzip2-devel libffi-devel -y
cd /usr/src
sudo wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
sudo tar xzf Python-3.11.9.tgz
cd Python-3.11.9
sudo ./configure --enable-optimizations
sudo make altinstall
python3.11 --version
python -m venv myenv
source myenv/bin/activate

sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
newgrp docker
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version

exit

