import boto3
import sys
import getopt

DEFAULT_PROFILE = None
DEFAULT_REGION = None
DEFAULT_OUTPUT = 's3://aws-athena-query-results-513065973071-us-east-1/alter_table_log'
DEFAULT_DROP_MODE = 'yes'
USAGE_STR = 'Usage: partition_synch.py -t <database.table> [-p <profile>] [-r <region>] [-o <output>] [-d <yes|no> [-v]'


def main_session(argv):
    full_table_name, profile, region, output, drop, verbose = parse_parameters(argv)
    database, table = full_table_name.split('.')
    session_handle = boto3.session.Session(profile_name=profile, region_name=region)
    bucket, prefix, partition_name_list = get_bucket_prefix_partition(session_handle, database, table)
    partition_value_set = get_table_partitions(session_handle, database, table)
    folder_set = get_bucket_directories(session_handle, bucket, prefix)
    missing_partition_set = folder_set.difference(partition_value_set)
    extra_partition_set = partition_value_set.difference(folder_set)
    if verbose:
        print('\n',
              "Table:", table, '\n',
              "Database:", database, '\n',
              "Profile:", profile, '\n',
              "Output:", output, '\n',
              "Region:", region, '\n',
              "Bucket:", bucket, '\n',
              "Prefix:", prefix, '\n'
              "Drop Partitions:", drop)
        print("Partition Name:", partition_name_list)
        print("Partition Values:", sorted(list(partition_value_set)))
        print("Folders:         ", sorted(list(folder_set)))
        print("Partitions to add:", missing_partition_set)
        print("Partitions to drop:", extra_partition_set)
    if len(missing_partition_set) == 0 and len(extra_partition_set) == 0:
        print("The {}.{} table partitions are up-to-date with s3://{}/{}".format(database, table, bucket, prefix))
        exit(0)
    if len(missing_partition_set) > 0:
        print("Adding missing partitions to the {}.{} table from s3://{}/{}".format(database, table, bucket, prefix))
        for ddl_str in adjust_partitions("ADD", missing_partition_set, partition_name_list, database, table, bucket, prefix):
            if verbose:
                print("DDL String:", ddl_str)
            qid = execute_athena_command(session_handle, ddl_str, database, output)
            if verbose:
                print(qid)
    if len(extra_partition_set) > 0 and drop == 'yes':
        print("Dropping extra partitions to the {}.{} table from s3://{}/{}".format(database, table, bucket, prefix))
        for ddl_str in adjust_partitions("DROP", extra_partition_set, partition_name_list, database, table, bucket, prefix):
            if verbose:
                print("DDL String:", ddl_str)
            qid = execute_athena_command(session_handle, ddl_str, database, output)
            if verbose:
                print(qid)


def adjust_partitions(p_operation, p_diff_set, p_partition_name_list, p_database, p_table, p_bucket, p_prefix):
    # Can be changed to a single ADD / DROP for multiple partitions
    for new_folder in p_diff_set:
        new_folder_list = new_folder.split('/')
        partition_str = ''
        for i in range(len(new_folder_list)):
            if partition_str != '':
                partition_str += ', '
            partition_str += p_partition_name_list[i] + " = " + "'" + new_folder_list[i] + "'"  # optimize

        ddl_str = "ALTER TABLE {0}.{1} {2} PARTITION ({3})".format(p_database, p_table, p_operation, partition_str)
        if p_operation == 'ADD':
            location = " LOCATION 's3://{0}/{1}{2}'".format(p_bucket, p_prefix, new_folder)
            ddl_str += location
        yield ddl_str


def parse_parameters(argv):
    table = None
    profile = DEFAULT_PROFILE
    region = DEFAULT_REGION
    output = DEFAULT_OUTPUT
    verbose = False
    drop = DEFAULT_DROP_MODE

    try:
        opts, args = getopt.getopt(argv, "hvt:p:r:o:d:", ["table=", "profile=", "region=", "output=", "drop="])
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
        elif opt in ("-d", "--drop"):
            drop = arg.lower() if arg.lower() in ('yes', 'no') else DEFAULT_DROP_MODE

    return table, profile, region, output, drop, verbose

def get_bucket_directories(p_session, p_bucket, p_prefix) -> set():
    s3_client = p_session.resource(service_name='s3')
    bucket = s3_client.Bucket(name=p_bucket)
    unique_dir = set()
    for obj in bucket.objects.filter(Prefix=p_prefix):
        part_dir = obj.key
        if part_dir[-1] == '/':  # skip folders
            continue
        part_dir = part_dir[len(p_prefix):]   # remove prefix
        part_dir = part_dir[:(part_dir.rfind('/'))]  # remove file name
        if part_dir != '':
            unique_dir.add(part_dir)
    return unique_dir


def get_table_partitions(p_session, p_database, p_table) -> set():
    glue_client = p_session.client(service_name='glue')
    partitions = glue_client.get_partitions(
        #       CatalogId='awsdatacatalog',
        DatabaseName=p_database,
        TableName=p_table
    )
    unique_part_values = set()
    for partition_list in partitions['Partitions']:
        unique_part_values.add('/'.join(partition_list['Values']))

    return unique_part_values


def get_bucket_prefix_partition(p_session, p_database, p_table):
    glue_client = p_session.client(service_name='glue')
    resp = glue_client.get_table(DatabaseName=p_database, Name=p_table)
    resp_list = resp['Table']['StorageDescriptor']['Location'].split('/')
    # Bucket name is always 3rd in the list s3://bucket_name/etc...
    l_bucket = resp_list[2]
    # Parse out path to table location in the bucket ("local prefix") appending with / if it is missing
    l_prefix = "/".join(resp_list[3:]) + ('' if resp['Table']['StorageDescriptor']['Location'][-1] == '/' else '/')
    l_partition_name_list = []
    for part_key in resp['Table']['PartitionKeys']:
        l_partition_name_list.append(part_key['Name'])

    return l_bucket, l_prefix, l_partition_name_list


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

