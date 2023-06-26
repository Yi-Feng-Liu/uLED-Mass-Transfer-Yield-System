FROM python:3.10.9

COPY pip.conf /etc/pip.conf
# install env and tools
RUN pip install -U pip && pip install numpy pandas matplotlib opencv-contrib-python scikit-learn seaborn schedule scipy tqdm zeep paramiko==2.12.0 python-pptx==0.6.21
WORKDIR /ST
RUN set -eux && apt-get update
RUN apt-get install -y sshfs
COPY . .
RUN pip install pymongo psutil
COPY --chown=0:0 .ssh /root/.ssh
RUN chmod -R 600 /root/.ssh

# RUN pip install --upgrade pip && pip install -r requirements.txt

# using cache
RUN mkdir /UM && mkdir /UM100
WORKDIR /ST/programe
CMD sshfs -o ro,reconnect wma@l4afls01:/home/nfs/ledfs /UM && sshfs -o ro,reconnect wma@10.88.26.100:/mnt/raidpool/vm/user/MT/ftp/UM /UM100 && bash

# CMD ["python", "run.py"]
