chmod 400 /Users/naman/Downloads/algo-mumbai.pem
scp -i /Users/naman/Downloads/algo-mumbai.pem /Users/naman/Desktop/Projects/Algo-IIFL/jobs/config.py ec2-user@ec2-3-109-41-113.ap-south-1.compute.amazonaws.com:~/.
scp -i /Users/naman/Downloads/algo-mumbai.pem ~/.ssh/id_rsa* ec2-user@ec2-3-109-41-113.ap-south-1.compute.amazonaws.com:~/.ssh/