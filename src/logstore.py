from aliyun.log import *
import time


def sample_put_logs(client, project, logstore, compress=False):
    topic = 'TestTopic_2'
    source = ''
    contents = [
        ('key_1', 'key_1'),
        ('avg', '30')
    ]
    logitemList = []  # LogItem list
    logItem = LogItem()
    logItem.set_time(int(time.time()))
    logItem.set_contents(contents)
    for i in range(0, 1):
        logitemList.append(logItem)
    request = PutLogsRequest(project, logstore, topic,
                             source, logitemList, compress=compress)

    response = client.put_logs(request)
    response.log_print()
