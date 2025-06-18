# Setup of Echo-side VM

## 1. Openstack instance

- Login to https://openstack.stfc.ac.uk and add a large instance called something like `echo-monitor` from the Ubuntu Jammy image.
- Add a floating IP address.

## 2. Initiate microk8s

The Echo-side VM will use microk8s as a single-node k8s solution. This is installed using Canonical Snap.

```shell
sudo snap install microk8s --classic --channel=1.32
sudo microk8s enable cert-manager host-access ingress hostpath-storage dashboard
```

Setup aliases.

```shell
cat << EOF > ~/.bash_aliases 
alias mk8s='sudo microk8s'
alias kubectl='sudo microk8s kubectl'
alias helm='sudo microk8s helm'
EOF
```

## 3. Install and Configure Apache Airflow using Helm

### Installation

```shell
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```

### Configure Git-sync Sidecar

This is based on the [Airflow Docs](https://airflow.apache.org/docs/helm-chart/1.7.0/manage-dags-files.html#mounting-dags-from-a-private-github-repo-using-git-sync-sidecar)

Example gitsync-values.yaml
```yaml
dags:
  gitSync:
    enabled: true
    repo: git@github.com:lsst-uk/csd3-echo-somerville.git
    branch: main
    subPath: "echo-side/dags"
    sshKeySecret: airflow-ssh-secret
    knownHosts: |
      github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
      github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
      github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
extraSecrets:
  airflow-ssh-secret:
    data: |
      gitSshKey: |
        LS0tLS1CRUdJTiBPU...
```

where the GitHub SSH keys can be found [here](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints),
the `gitSshKey` (here incomplete) is a base64 encoded RSA private key generated following the [Airflow Docs](https://airflow.apache.org/docs/helm-chart/1.7.0/manage-dags-files.html#mounting-dags-from-a-private-github-repo-using-git-sync-sidecar)
and `repo`, `branch` and `subPath` can be modified to the location of your Apache Airflow DAGs on GitHub, in this case pointing to a folder in this repo: [https://github.com/lsst-uk/csd3-echo-somerville/tree/main/echo-side/dags](https://github.com/lsst-uk/csd3-echo-somerville/tree/main/echo-side/dags).

Then apply this to the Airflow installation with:

```shell
helm upgrade --install airflow apache-airflow/airflow --namespace airflow -f gitsync-values.yaml
```

Thereafter, on logging into the Airflow webUI, DAG Python scripts from the GitHub repo will appear in the DAG list.
