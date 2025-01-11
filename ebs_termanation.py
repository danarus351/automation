import boto3


def list_volumes_without_tag(ec2_client, tag_key):
    response = ec2_client.describe_volumes()
    volumes_without_tag = []

    for volume in response['Volumes']:
        if 'Tags' not in volume or tag_key not in volume['Tags'] or volume['State'] != 'in-use':
            volumes_without_tag.append(volume['VolumeId'])

    return volumes_without_tag


def delete_volumes(ec2_client, volume_ids):
    if volume_ids:
        for volume_id in volume_ids:
            ec2_client.delete_volume(VolumeId=volume_id)
            print(f"Deleted volume: {volume_id}")
    else:
        print("No volumes to delete.")


def main():
    session = boto3.Session(profile_name='<default profile>', region_name='default region')
    ec2_client = session.client('ec2')
    tag_key = {'Key': 'status', 'Value': 'in_use'}  # Replace with your actual tag key

    volumes_without_tag = list_volumes_without_tag(ec2_client, tag_key)

    if volumes_without_tag:
        print("Volumes without the tag '{}':".format(tag_key))
        for volume_id in volumes_without_tag:
            print(volume_id)

        delete = input("Do you want to delete these volumes? (yes/no): ").strip().lower()
        if delete == 'yes':
            ...
            delete_volumes(ec2_client, volumes_without_tag)
        else:
            print("No volumes were deleted.")
    else:
        print("All volumes have the tag '{}'.".format(tag_key))


if __name__ == "__main__":
    main()
