import boto3
import csv

# Initialize the EC2 client
session = boto3.Session(profile_name='<default profile>', region_name='default region')
ec2 = session.client('ec2')

volumes = ec2.describe_volumes()['Volumes']
for volume in volumes:
    pass

with open(f'volumes_with_tags_{session.region_name}.csv', 'w', newline='') as csvfile:
    fieldnames = ['Name','VolumeID', 'VolumeType', 'Size', 'SnapshotId', 'CreateTime', 'State', 'AvailabilityZone']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for volume in volumes:
        try:
            for tag in volume['Tags']:
                if tag['Key'] == 'Name':
                    name = tag['Value']
        except KeyError:
            name = ''
        row = {
            'VolumeID': volume['VolumeId'],
            'VolumeType': volume['VolumeType'],
            'Size': volume['Size'],
            'SnapshotId': volume['SnapshotId'],
            'CreateTime': volume['CreateTime'],
            'State': volume['State'],
            'AvailabilityZone': volume['AvailabilityZone'],
            'Name': name,
        }
        writer.writerow(row)