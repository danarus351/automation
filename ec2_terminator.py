import boto3


def list_instances_without_tag(ec2_client, tag_dic):
    response = ec2_client.describe_instances()
    instances_without_tag = []

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if ('Tags' not in instance or tag_dic not in instance['Tags']) and instance['State']['Name'] != 'terminated' :
                instances_without_tag.append(instance['InstanceId'])
    return instances_without_tag


def terminate_instances(ec2_client, instance_ids):
    for instance_id in instance_ids:
        try:
            ec2_client.modify_instance_attribute(
                InstanceId=instance_id,
                DisableApiTermination={'Value': False}
            )
        except:
            print('Failed to terminate instance', instance_id)
    if instance_ids:
        ec2_client.terminate_instances(InstanceIds=instance_ids)
        print(f"Terminated instances: {', '.join(instance_ids)}")
    else:
        print("No instances to terminate.")


def main():
    session = boto3.Session(profile_name='<default profile>', region_name='default region')
    ec2_client = session.client('ec2')
    tag_key = {'Key': 'status', 'Value': 'in_use'}

    instances_without_tag = list_instances_without_tag(ec2_client, tag_key)

    if instances_without_tag:
        print("Instances without the tag '{}':".format(tag_key))
        for instance_id in instances_without_tag:
            print(instance_id)

        terminate = input("Do you want to terminate these instances? (yes/no): ").strip().lower()
        if terminate == 'yes':
            terminate_instances(ec2_client, instances_without_tag)
        else:
            print("No instances were terminated.")
    else:
        print("All instances have the tag '{}'.".format(tag_key))


if __name__ == "__main__":
    main()
