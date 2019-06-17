import os

PROJECT = 'grass-clump-479'
INSTANCE = 'python-write-2'
REGION = 'us-central1'
STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'
SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\bigtableio-0.3.123.tar.gz'
# EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.123.tar.gz'

path = 'c:\\git\\beam-MF\\sdks\\python\\apache_beam\\io\\gcp\\'
args = '--{} {} --{} {} --{} {} --{} {} --{} {} --{} {} --{} {}'\
	.format(
			'project', PROJECT,
			'instance', INSTANCE,
			'region', REGION,
			'staging_location', STAGING_LOCATION,
			'temp_location', TEMP_LOCATION,
			'setup_file', SETUP_FILE,
			'extra_package', EXTRA_PACKAGE
	)
os.system('python {}bigtableio_it_test.py {}'.format(path, args))