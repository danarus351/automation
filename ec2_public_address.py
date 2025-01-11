import boto3
import csv

# Initialize the EC2 client
session = boto3.Session(profile_name='<default profile>', region_name='default region')
ec2 = session.client('ec2')


addresses = ec2.describe_addresses()['Addresses']

with open(f'address_with_tags_{session.region_name}.csv', 'w', newline='') as csvfile:
    fieldnames = ['Name','PublicIp','AssociationId', 'NetworkBorderGroup']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for address in addresses:
        try:
            for tag in address['Tags']:
                if tag['Key'] == 'Name':
                    name = tag['Value']
        except KeyError:
            name = ''
        try:
            association_id = address['AssociationId']
        except KeyError:
            association_id = ''
        row = {
            'Name': name,
            'PublicIp': address['PublicIp'],
            'AssociationId': association_id,
            'NetworkBorderGroup': address['NetworkBorderGroup']
        }
        writer.writerow(row)
