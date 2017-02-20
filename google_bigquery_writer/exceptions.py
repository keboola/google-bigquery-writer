class UserException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class ApplicationException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message
