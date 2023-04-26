steps to install jenkins:

1. sudo apt-get install default-jre 
# It is dependency of jenkins to install java.

2. wget -q -O - https://pkg.jenkins.io/debian-stable/jenkins.io.key | sudo apt-key add -
# Download the latest Jenkins package for Ubuntu and add Jenkins to the list of trusted repositories.

3. sudo sh -c 'echo deb http://pkg.jenkins.io/debian-stable binary/ > /etc/apt/sources.list.d/jenkins.list'
# to add the repository to your system's sources list.

4. sudo apt-get update
# Update the package list on your system 

5. s
# Install Jenkins

6. sudo systemctl start jenkins
# Start the Jenkins service 

7.  http://localhost:8080/
# check if it is running



EOD 24-04-2023:-
- Learn about Redshift
- Learn basics of S3 bucket
- Try to make a connection with redshift but getting error