# Initialise VM

Install microk8s, docker and airflow.

## Install microk8s
```
sudo snap install microk8s --classic --channel=1.28
sudo usermod -a -G microk8s $USER
sudo chown -f -R $USER ~/.kube
```
_Login again to initialise group membership_

### Check and enable features
```
microk8s status --wait-ready
microk8s enable dashboard
microk8s enable host-access
microk8s enable hostpath-storage
microk8s kubectl get pods -A
```

## Install docker (from https://docs.docker.com/engine/install/ubuntu/)
```
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo docker run hello-world
```

### Optional post-install (removes need for sudo docker ...) (from https://docs.docker.com/engine/install/linux-postinstall/)
```
sudo groupadd docker 2>/dev/null
sudo usermod -aG docker $USER
newgrp docker
docker run hello-world
sudo chown "$USER":"$USER" /home/"$USER"/.docker -R 2>/dev/null
sudo chmod g+rwx "$HOME/.docker" -R 2>/dev/null
```

### Start on boot
```
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
```

## Install Apache airflow
```
microk8s helm repo add apache-airflow https://airflow.apache.org
microk8s helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace > airflow_install.log
```

## Test installation
```
microk8s kubectl get pods -A
```
