import boto3
import json
import pandas as pd
from pandas.io.json import json_normalize

ebs = boto3.client('ebs')
ec2 = boto3.client('ec2')

# get sorted list of snapshots
snapshots = ec2.describe_snapshots(OwnerIds=['self'])
df = pd.DataFrame.from_dict(snapshots['Snapshots'])
df.sort_values(by=['OwnerId', "VolumeId", "StartTime"], inplace = True)

# per volumeid lineage, get for each one changed blocks
v_prev = ''
sid_prev = ''
for index, row in df.iterrows():
    #Break StartTime into individual date and time
    date = str(row['StartTime']).split('+')[0].split('.')[0].split(' ')[0] #Strip milliseconds and offset
    time = str(row['StartTime']).split('+')[0].split('.')[0].split(' ')[1] #Strip milliseconds and offset

    if row['VolumeId'] == v_prev: # If current volume ID matches previous volume ID we are working on the same volume
        # Get changed block size between this snapshot and the previous snapshot for the same volume
        changed_blocks = ebs.list_changed_blocks(FirstSnapshotId = sid_prev, SecondSnapshotId = row['SnapshotId'], MaxResults = 10000)
        block_size = changed_blocks['BlockSize'] # Let EBS tell us the blocksize of the snapshot
        changed = len(changed_blocks['ChangedBlocks'])
        while 'NextToken' in changed_blocks: # If NextToken is present than we have more block changes to get
            changed_blocks = ebs.list_changed_blocks(FirstSnapshotId = sid_prev,SecondSnapshotId = row['SnapshotId'], MaxResults = 10000, NextToken = changed_blocks['NextToken'])
            changed += len(changed_blocks['ChangedBlocks'])
    else:
        sid_prev = 'snap-00000000000000000' # There is no previous snapshot SID for this volume
        # Get total number of blocks in the first snapshot for this volume
        changed_blocks = ebs.list_snapshot_blocks(SnapshotId=row['SnapshotId'], MaxResults=10000)
        block_size = changed_blocks['BlockSize'] # Let EBS tell us the blocksize of the snapshot
        changed = len(changed_blocks['Blocks'])
        while 'NextToken' in changed_blocks: # If NextToken is present than we have more block changes to get
            changed_blocks = ebs.list_snapshot_blocks(SnapshotId=row['SnapshotId'], MaxResults=10000, NextToken=changed_blocks['NextToken'])
            changed += len(changed_blocks['Blocks'])
    # Output the results
    print(date + ',' + time + ',' + row['VolumeId'] + "," + sid_prev + "," + row['SnapshotId'] + "," + str(changed * block_size))
    # Store the current volumeId and snapshotId for next iteration
    v_prev = row['VolumeId']
    sid_prev = row['SnapshotId']
