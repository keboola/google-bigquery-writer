import sys
import traceback
from google_bigquery_writer.exceptions \
    import UserException, ApplicationException
from google_bigquery_writer.app import App


def main():
    try:
        application = App()
        application.run()
        sys.exit(0)
    except UserException as err:
        message = '%s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    except ApplicationException as err:
        message = '%s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)
    except Exception as err:
        message = '%s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)


if __name__ == '__main__':
    main()
