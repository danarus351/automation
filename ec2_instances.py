import boto3
import csv
# Initialize the EC2 client
session = boto3.Session(profile_name='<default profile>',region_name='default region')
ec2 = session.client('ec2')

# Retrieve the instances and their tags
instances = ec2.describe_instances()

# Create a list of unique tag keys across all instances
tag_keys = set()
for reservation in instances['Reservations']:
    for instance in reservation['Instances']:
        if 'Tags' in instance:
            for tag in instance['Tags']:
                tag_keys.add(tag['Key'])

# Create a CSV file and write the header row
with open(f'instances_with_tags_{session.region_name}', 'w', newline='') as csvfile:
    fieldnames = ['InstanceId', 'InstanceType', 'State', 'Zone', 'PrivateIpAddress', 'PrivateDnsName', 'PublicDnsName'] + list(tag_keys)
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()



    # Write data rows
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            row = {
                'InstanceId': instance['InstanceId'],
                'InstanceType': instance['InstanceType'],
                'State': instance['State']['Name'],
                'Zone': instance['Placement']['AvailabilityZone'],
                'PrivateIpAddress': instance['PrivateIpAddress'],
                'PrivateDnsName': instance['PrivateDnsName'],
                'PublicDnsName': instance['PublicDnsName'],
            }

            if 'Tags' in instance:
                for tag in instance['Tags']:
                    row[tag['Key']] = tag['Value']

            writer.writerow(row)
