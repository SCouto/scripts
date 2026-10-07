#!/usr/bin/env python3
"""
n8n EC2 Instance Manager
Interactive script to connect to n8n EC2 instances via SSM.
"""

import sys
import subprocess
import argparse
import time
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import inquirer


TARGET_NAMES = [
    "n8n-main",
    "n8n-worker",
]

DEFAULT_PROFILE = "pro"


def aws_sso_login(profile):
    print(f"Authenticating with AWS SSO (profile: {profile})...")
    try:
        subprocess.run(
            ['aws', 'sso', 'login', '--profile', profile],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"\nError: Failed to authenticate with AWS SSO: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\nError: AWS CLI not found. Please install it first.")
        sys.exit(1)


def get_ec2_instances(profile):
    try:
        session = boto3.Session(profile_name=profile)
        ec2_client = session.client('ec2')
    except NoCredentialsError:
        print("Error: AWS credentials not configured.")
        print(f"Please run 'aws sso login --profile {profile}' first.")
        sys.exit(1)

    try:
        response = ec2_client.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': TARGET_NAMES},
                {'Name': 'instance-state-name', 'Values': ['running', 'stopped', 'pending']},
            ]
        )

        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                name = next(
                    (tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'),
                    ''
                )
                instances.append({
                    'id': instance['InstanceId'],
                    'name': name,
                    'state': instance['State']['Name'],
                })

        return instances

    except ClientError as e:
        print(f"Error querying EC2 instances: {e}")
        sys.exit(1)


def display_instance_menu(instances):
    if not instances:
        print("No instances found matching target names.")
        print(f"Looking for: {', '.join(TARGET_NAMES)}")
        return []

    running = [i for i in instances if i['state'] == 'running']

    if not running:
        print("No running instances found.")
        print(f"\nFound {len(instances)} instance(s):")
        for inst in instances:
            print(f"  - {inst['name']} ({inst['id']}) - {inst['state']}")
        return []

    sorted_instances = sorted(running, key=lambda i: TARGET_NAMES.index(i['name']) if i['name'] in TARGET_NAMES else 999)

    choices = [f"{inst['name']} ({inst['id']})" for inst in sorted_instances]
    choices.append('[Exit]')

    questions = [
        inquirer.Checkbox(
            'instances',
            message="Select n8n instances (Space=select, Enter=confirm)",
            choices=choices,
        ),
    ]

    answers = inquirer.prompt(questions)

    if not answers or not answers['instances']:
        return []

    selected = answers['instances']

    if '[Exit]' in selected:
        print("\nExiting.")
        return []

    instance_ids = []
    for item in selected:
        if item != '[Exit]':
            instance_id = item.split('(')[1].split(')')[0]
            instance_ids.append(instance_id)

    return instance_ids


def open_iterm_split_pane(instance_id, instance_name, profile):
    ssm_command = f"aws ssm start-session --profile {profile} --target {instance_id}"

    script = f'''
    tell application "iTerm"
        tell current window
            tell current session
                set newSession to (split horizontally with default profile)
            end tell
            tell newSession
                write text "{ssm_command}"
            end tell
        end tell
    end tell
    '''

    try:
        subprocess.run(['osascript', '-e', script], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to open iTerm split pane for {instance_name}: {e}")
    except FileNotFoundError:
        print("Error: osascript not found. Are you on macOS?")


def connect_to_instances(instance_ids, instances, profile):
    if not instance_ids:
        print("\nNo instances selected. Exiting.")
        return

    id_to_name = {inst['id']: inst['name'] for inst in instances}

    if len(instance_ids) == 1:
        instance_id = instance_ids[0]
        instance_name = id_to_name.get(instance_id, 'Unknown')
        print(f"\nConnecting to {instance_name} ({instance_id})...")

        try:
            subprocess.run(
                f"aws ssm start-session --profile {profile} --target {instance_id}",
                shell=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to connect to {instance_name}: {e}")
            sys.exit(1)
    else:
        print(f"\nOpening {len(instance_ids)} split panes (profile: {profile})...")

        for instance_id in instance_ids:
            instance_name = id_to_name.get(instance_id, 'Unknown')
            print(f"  - {instance_name} ({instance_id})")
            open_iterm_split_pane(instance_id, instance_name, profile)
            time.sleep(0.3)

        print(f"\nOpened {len(instance_ids)} split pane(s).")


def main():
    parser = argparse.ArgumentParser(
        description='n8n EC2 Instance Manager - Connect via AWS SSM'
    )
    parser.add_argument(
        '--profile',
        type=str,
        default=DEFAULT_PROFILE,
        help=f'AWS profile name (default: {DEFAULT_PROFILE})',
    )
    args = parser.parse_args()

    print("EC2 Instance Manager - n8n Instances\n")

    aws_sso_login(args.profile)
    print()

    print("Fetching EC2 instances...")
    instances = get_ec2_instances(args.profile)

    instance_ids = display_instance_menu(instances)
    connect_to_instances(instance_ids, instances, args.profile)


if __name__ == '__main__':
    main()
