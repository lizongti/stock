FROM python:3.9.7

COPY requirements.txt /
RUN pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ \
    && ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo Asia/Shanghai > /etc/timezone && \
    sed -i -e 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf

ADD . /opt/stock

ENV PYTHONUNBUFFERED 1

WORKDIR /opt/stock

ENTRYPOINT [ "python3", "scheduler.py"]