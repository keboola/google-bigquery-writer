import sys
import traceback
from google_bigquery_writer.exceptions import UserException, ApplicationException
from google_bigquery_writer.app import App

try:
    application = App(data_dir='/data')
    application.run()
    sys.exit(0)
except UserException as err:
    message = 'User exception: %s' % err
    print(message, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
except ApplicationException as err:
    message = 'Application exception: %s' % err
    print(message, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(2)
except Exception as err:
    message = 'Unhandled exception: %s' % err
    print(message, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(2)

