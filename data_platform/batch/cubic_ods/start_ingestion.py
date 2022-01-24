
import logging
import time


def run():
  # sample process
  logging.error('START start_ingestion')
  time.sleep(10)
  logging.error('END start_ingestion (Took 10 seconds)')

if __name__ == '__main__':
  run()
