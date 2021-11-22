FROM ubuntu:18.04

# RUN apt-get update \
#     && apt-get install -y software-properties-common curl \
#     && add-apt-repository ppa:jonathonf/python-3.6 \
#     && apt-get remove -y software-properties-common \
#     && apt autoremove -y \
#     && apt-get update \
#     && apt-get install -y python3.6 \
#     && curl -o /tmp/get-pip.py "https://bootstrap.pypa.io/get-pip.py" \
#     && python3.6 /tmp/get-pip.py \
#     && apt-get remove -y curl \
#     && apt autoremove -y \
#     && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get install -y locales
RUN locale-gen ko_KR.UTF-8
ENV LC_ALL ko_KR.UTF-8
RUN apt-get install curl -y \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
#RUN python get-pip.py
#RUN curl https://bootstrap.pypa.io/ez_setup.py -o - | python3.6 && python3.6 -m easy_install pip
RUN apt-get update \
    && apt-get install -y python3.6-venv
RUN apt-get install python3-pip -y \
    && python3.6 -m pip install pip --upgrade \
    && python3.6 -m pip install wheel
RUN apt-get install git -y \
    && apt install git

COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt \
    && pip install git+https://github.com/lovit/KR-WordRank.git


SHELL [ "/bin/bash" ]
