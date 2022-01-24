
import signal

from data_platform.batch.cubic_ods import process_incoming


def run():
  # kill switch state
  kill = {
    'cubic_ods__process_incoming': False
  }
  # kill switch handler
  def sigHandler(signum, frame):
    kill['cubic_ods__process_incoming'] = True

  # register handlers for termination signals, so we don't kill in mid process
  signal.signal(signal.SIGINT, sigHandler) # SINGINT 2 (ctrl-c)
  signal.signal(signal.SIGTERM, sigHandler) # SIGTERM 15 (docker stop)

  # if we haven't killed the process, contiue calling it
  while not kill.get('cubic_ods__process_incoming', False):
    process_incoming.run()


if __name__ == '__main__':
  run()
