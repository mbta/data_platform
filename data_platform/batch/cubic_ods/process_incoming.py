
import logging
import time


def run():
  # sample process
  logging.error('START process_incoming')
  time.sleep(10)
  logging.error('END process_incoming (Took 10 seconds)')

if __name__ == '__main__':
  run()
