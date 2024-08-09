sudo dnf update -y
sudo dnf install -y git
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
git clone git@github.com:Naman101199/Algo-IIFL.git
mv config.py Algo-IIFL/utils
