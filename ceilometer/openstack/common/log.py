def log(msg):
    print(msg)

class logger(object):
    def info(self, msg):
        print("INFO: {}".format(msg))
    def debug(self, msg):
        print("DEBUG: {}".format(msg))


def getLogger(loggername):
    return logger()
