export runtest := '''
  python3 -mvenv --system-site-packages /mnt/venv
  /mnt/venv/bin/pip install --upgrade wheel
  /mnt/venv/bin/pip install --upgrade pip
  /mnt/venv/bin/pip install "/mnt/app[test]"
  cd /mnt/app
  /mnt/venv/bin/python -munittest
'''

yum-setup := '''
  yum update -y
  yum install -y python3 sqlite3 python3-pip python3-venv
'''

dnf-setup := '''
  dnf update -y
  dnf install -y python3 sqlite python3-pip
'''

apt-setup := '''
  apt update
  apt upgrade -y
  apt install -y python3 sqlite3 python3-pip python3-venv
'''

apk-setup := '''
  apk update
  apk upgrade
  apk add python3 sqlite
'''

all: test

test: python-tests debian-tests ubuntu-tests centos-tests fedora-tests rhel-tests alpine-tests

run-test $container $setup="":
  #!/bin/sh
  set -eux
  
  podman container run \
    --rm \
    -e PYTHONPATH=/mnt/app/src \
    --mount type=volume,destination=/mnt/venv \
    --mount type=bind,source=.,destination=/mnt/app,ro=true \
    --security-opt label=disable \
    "docker.io/$container" \
    /bin/sh -c "
      set -euvf

      $setup
      $runtest
    "

test-python version="latest": (run-test ("python:" + version))

python-tests: (test-python "3.6") (test-python "3.7") (test-python "3.8") (test-python "3.9") (test-python "3.10") (test-python "3.11")

test-centos version="latest": (run-test ("centos:" + version) yum-setup)
centos-tests: (test-centos "7")

test-alpine version="latest": (run-test ("alpine:" + version) apk-setup)
alpine-tests: (test-alpine "3.14") (test-alpine "3.15") (test-alpine "3.16") (test-alpine "3.17")

test-apt image="debian:latest": (run-test image apt-setup)

test-debian version="latest": (test-apt ("debian:" + version))
test-ubuntu version="latest": (test-apt ("ubuntu:" + version))

debian-tests: (test-debian "buster") (test-debian "bullseye") (test-debian "bookworm")
ubuntu-tests: (test-ubuntu "18.04") (test-ubuntu "20.04") (test-ubuntu "22.04")

test-dnf image="fedora:latest": (run-test image dnf-setup)

test-fedora version="latest": (test-dnf ("fedora:" + version))
fedora-tests: (test-fedora "36") (test-fedora "37") (test-fedora "38")

test-rhel version="ubi9": (test-dnf ("redhat/" + version))

rhel-tests: (test-rhel "ubi8") (test-rhel "ubi9")
