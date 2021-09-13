FROM python:3.9.7

COPY requirements.txt /
RUN pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

ADD . /opt/stock

WORKDIR /opt/stock

ENTRYPOINT [ "python3", "scheduler.py"]