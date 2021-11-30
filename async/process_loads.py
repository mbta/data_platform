

def main(dryRun=True):

  ## load objects

  # get all tables that utilize object loads
  # select * from tables
  tables = [
    {
      'id': 1,
      'name': 'EDW.TRANSACTION_HISTORY'
    }
  ]

  for table in tables:
    # get last load record for table
    # select * from metadata__ct_loads where table_id = {table.id} order by created desc limit 1
    lastLoad = {
      'id': 1,
      'object_key': 'cubic_qlik_ingest/EDW.TRANSACTION_HISTORY__ct/20211119-052743447.csv.gz'
      'status': 'processed'
    }

    # if the last load has been processed, it should be ok to check S3 for more loads
    # note: this is more of an optimization to save us from hitting S3 all the time, which is still OK
    if lastLoad.get('status', '') == 'processed':
      # check to see if there are more objects to load
      s3 = boto3.client('s3')
      bucket = 'cubic_qlik'

      objects = s3.list_objects(Bucket='cubic_qlik', Prefix=lastLoad.get('object_key'))
      for obj in objects:
        pass


  ## run jobs


response = client.start_job_run(
  JobName = 'cubic_qlik_import_cdc',
  Arguments = {
    '--day_partition_key':   'partition_0',
    '--hour_partition_key':  'partition_1',
    '--day_partition_value':  day_partition_value,
    '--hour_partition_value': hour_partition_value
  }
)




  if dryRun:
    pass
  else:
    pass

if __name__ == '__main__':

  dryRun = False

  main(dryRun=dryRun)
