import re

class ApacheAccessLog():
    """
     This class represents an Apache access log line.
     See http://httpd.apache.org/docs/2.2/logs.html for more details.
    """

    # Example Apache log line:
    # 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
    # 1:IP  2:client 3:user 4:datetime 5:method 6:req 7:proto 8:respcode 9:size
    # LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"
    log_entry_regex = '(.*?) (.*?) (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d{3}) (.*?)'

    def __init__(self, ip,  client_identd,  user_id, dateTime,  method,  endpoint, protocol,  response_code, content_size):
        self.ip = ip
        self.client_identd = client_identd
        self.user_id = user_id
        self.date_time_string = dateTime
        self.method = method
        self.endpoint = endpoint
        self.protocol = protocol
        self.response_code = response_code
        self.content_size = content_size
    
    @staticmethod
    def parse_from_log_line(logline):
        m = re.search(ApacheAccessLog.log_entry_regex, logline)
        if m is None:
            print("Cannot parse logline" + logline)
            return None
        return ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9))

    def __repr__(self):
        return "{} {} {} [{}] \"{} {} {}\" {} {}".format(self.ip, self.client_identd, self.user_id, self.date_time_string, self.method, self.endpoint,self.protocol, self.response_code, self.content_size)