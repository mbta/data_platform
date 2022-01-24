
import signal

from data_platform.batch.cubic_ods import start_ingestion


def run():
  # kill switch state
  kill = {
    'cubic_ods__start_ingestion': False
  }
  # kill switch handler
  def sigtermHandler(signum, frame):
    kill['cubic_ods__start_ingestion'] = True

  # register handlers for termination signals, so we don't kill in mid process
  signal.signal(signal.SIGINT, sigtermHandler) # SINGINT 2 (ctrl-c)
  signal.signal(signal.SIGTERM, sigtermHandler) # SIGTERM 15 (docker stop)

  # if we haven't killed the process, contiue calling it
  while not kill.get('cubic_ods__start_ingestion', False):
    start_ingestion.run()


if __name__ == '__main__':
  run()
