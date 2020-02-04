import boto3
import sys
import getopt


# BUCKET = 'gsdp-segment-logs'
# PREFIX = 'segment-logs/HpDYYfmrXr/'
# DATABASE = 'gsdp_segment'
# TABLE = 'hpdyyfmrxr_test'
# PARTITION_NAME = 'partition_0'
# PROFILE = 'vmware'
DEFAULT_PROFILE = None
DEFAULT_REGION = None
DEFAULT_OUTPUT = 's3://aws-athena-query-results-513065973071-us-east-1/alter_table_log'
USAGE_STR = 'Usage: parition_fix.py -t <database.table> [-p <profile>] [-r <region>] [-o <output>]'


def main_session(argv):
    full_table_name, profile, region, output, verbose = parse_parameters(argv)
    database, table = full_table_name.split('.')
    session_handle = boto3.session.Session(profile_name=profile, region_name=region)
    partition_set = get_table_partitions(session_handle, database, table)
    bucket, prefix, partition = get_bucket_prefix_partition(session_handle, database, table)
    folder_set = get_bucket_directories(session_handle, bucket, prefix)
    diff_set = folder_set.difference(partition_set)
    if verbose:
        print('Table:', table)
        print('Database:', database)
        print('Profile:', profile)
        print('Output:', output)
        print('Region:', region)
        print('Bucket', bucket)
        print('Prefix:', prefix)
        print('Partition Name:', partition)
        print('Partition Value(s):', diff_set)
    if len(diff_set) == 0:
        print("The {}.{} table partitions are up-to-date with s3://{}/{}".format(database, table, bucket, prefix))
        exit(0)
    else:
        print("Adding missing partitions to the {}.{} table from s3://{}/{}".format(database, table, bucket, prefix))
    for new_partition in diff_set:
        ddl_str = "ALTER TABLE {0}.{1} ADD PARTITION ({2} = '{3}') " \
                  " LOCATION 's3://{4}/{5}{3}'".format(database, table, partition, new_partition, bucket, prefix)
        if verbose:
            print(ddl_str)
        qid = execute_athena_command(session_handle, ddl_str, database, output)
        if verbose:
            print(qid)


def parse_parameters(argv):
    table = None
    profile = DEFAULT_PROFILE
    region = DEFAULT_REGION
    output = DEFAULT_OUTPUT
    verbose = False

    try:
        opts, args = getopt.getopt(argv, "hvt:p:r:o:", ["table=", "profile=", "region=", "output="])
    except getopt.GetoptError:
        print(USAGE_STR)
        sys.exit(1)
    for opt, arg in opts:
        if opt == '-h':
            print(USAGE_STR)
            sys.exit(0)
        elif opt == '-v':
            verbose = True
        elif opt in ("-t", "--table"):
            table = arg
        elif opt in ("-p", "--profile"):
            profile = arg
        elif opt in ("-r", "--region"):
            region = arg
        elif opt in ("-o", "--output"):
            output = arg

    return table, profile, region, output, verbose

def get_bucket_directories(p_session, p_bucket, p_prefix) -> set():
    s3_client = p_session.resource(service_name='s3')
    bucket = s3_client.Bucket(name=p_bucket)
    unique_dir = set()
    for obj in bucket.objects.filter(Prefix=p_prefix):
        part_dir = obj.key[len(p_prefix):]   # remove prefix
        part_dir = part_dir[:(part_dir.rfind('/'))]  # remove file name
        unique_dir.add(part_dir)

    return unique_dir


def get_table_partitions(p_session, p_database, p_table) -> set():
    glue_client = p_session.client(service_name='glue')
    partitions = glue_client.get_partitions(
        #       CatalogId='awsdatacatalog',
        DatabaseName=p_database,
        TableName=p_table
    )
    unique_part = set()
    for partition_list in partitions['Partitions']:
        for partition in partition_list['Values']:
            unique_part.add(partition)

    return unique_part


def get_bucket_prefix_partition(p_session, p_database, p_table):
    glue_client = p_session.client(service_name='glue')
    resp = glue_client.get_table(DatabaseName=p_database, Name=p_table)
    resp_list = resp['Table']['StorageDescriptor']['Location'].split('/')
    l_prefix = "/".join(resp_list[3:]) + ('' if resp['Table']['StorageDescriptor']['Location'][-1] == '/' else '/')

    return resp_list[2], l_prefix, resp['Table']['PartitionKeys'][0]['Name']


def execute_athena_command(p_session, p_command, p_database, p_output):
    athena_client = p_session.client(service_name='athena')
    response = athena_client.start_query_execution(
        QueryString=p_command,
        QueryExecutionContext={'Database': p_database},
        ResultConfiguration={'OutputLocation': p_output}
    )
    return response

if __name__ == '__main__':
    main_session(sys.argv[1:])

