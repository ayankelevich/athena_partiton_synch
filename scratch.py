import boto3

PROFILE = 'vmware'
DATABASE = 'gsdp_segment'
TABLE = 'prefix_level_2'
profile = PROFILE
p_database = DATABASE
p_table = TABLE
session_handle = boto3.session.Session(profile_name=profile)

# glue_client = session_handle.client(service_name='glue')
# partitions = glue_client.get_partitions(
#     #       CatalogId='awsdatacatalog',
#     DatabaseName=p_database,
#     TableName=p_table
# )
#
# part_set = set()
# for partition_list in partitions['Partitions']:
#     level = 0
#     path = ''
#     for partition in partition_list['Values']:
#         path += '/' + partition
#         part_set.add((level, path))
#         level += 1
# print('Here')
# for i in part_set:
#     if i[0] == 2:
#         print(i)

glue_client = session_handle.client(service_name='glue')
resp = glue_client.get_table(DatabaseName=p_database, Name=p_table)
resp_list = resp['Table']['StorageDescriptor']['Location'].split('/')

p_prefix = "/".join(resp_list[3:]) + ('' if resp['Table']['StorageDescriptor']['Location'][-1] == '/' else '/')
bucket = resp_list[2]
print('Bucket:', bucket, 'Prefix:', p_prefix)

level = 0
part_name_set = set()
for part_key in resp['Table']['PartitionKeys']:
    part_name_set.add((level, part_key['Name']))
    level += 1

print(part_name_set)

s3_client = session_handle.resource(service_name='s3')
bucket = s3_client.Bucket(name=bucket)
unique_dir = set()
for obj in bucket.objects.filter(Prefix=p_prefix):
    part_dir = obj.key
    if part_dir[-1] == '/':  # skip folders
        continue
    part_dir = part_dir[len(p_prefix):]   # remove prefix
    part_dir = part_dir[:(part_dir.rfind('/'))]  # remove file name
    if part_dir != '':
#        print(part_dir)
        unique_dir.add(part_dir)

unique_dir_list = list(unique_dir)
unique_dir_list.sort()
print(unique_dir_list)

partitions = glue_client.get_partitions(
    #       CatalogId='awsdatacatalog',
    DatabaseName=p_database,
    TableName=p_table
)
unique_part = set()
for partition_list in partitions['Partitions']:
#    print("Partition List:", partition_list)
#    for partition in partition_list['Values']:
    unique_part.add('/'.join(partition_list['Values']))

unique_part_list = list(unique_part)
unique_part_list.sort()
print(unique_part_list)