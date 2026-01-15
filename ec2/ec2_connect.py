#!/usr/bin/env python3
"""
EC2 Instance Manager
Interactive script to list and connect to Airflow EC2 instances.
"""

import sys
import subprocess
import argparse
import time
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import inquirer


# Target instance names to filter
TARGET_NAMES = [
    "airflow-scheduler",
    "airflow-worker",
    "airflow-triggerer",
    "airflow-webserver"
]


def aws_sso_login(profile):
    """
    Perform AWS SSO login for the specified profile.
    Always runs 'aws sso login --profile <profile>'.
    """
    print(f"Authenticating with AWS SSO (profile: {profile})...")
    print("This will open a browser window for authentication.\n")

    try:
        subprocess.run(
            ['aws', 'sso', 'login', '--profile', profile],
            check=True
        )
        print(f"\nSuccessfully authenticated with profile: {profile}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nError: Failed to authenticate with AWS SSO: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\nError: AWS CLI not found. Please install it first.")
        sys.exit(1)


def sort_instances_by_type(instances):
    """
    Sort instances by type: workers, schedulers, triggerers, webservers.
    Within each type, sort by instance ID.
    """
    type_order = {
        'worker': 1,
        'scheduler': 2,
        'triggerer': 3,
        'webserver': 4
    }

    def get_sort_key(inst):
        # Extract type from name (e.g., 'airflow-worker' -> 'worker')
        inst_type = inst['name'].replace('airflow-', '')
        return (type_order.get(inst_type, 999), inst['id'])

    return sorted(instances, key=get_sort_key)


def get_ec2_instances(profile=None):
    """
    Fetch EC2 instances matching the target names.
    profile: AWS profile name to use (optional)
    Returns list of dicts with instance details.
    """
    try:
        # Create session with profile if provided
        if profile:
            session = boto3.Session(profile_name=profile)
            ec2_client = session.client('ec2')
        else:
            ec2_client = boto3.client('ec2')
    except NoCredentialsError:
        print("Error: AWS credentials not configured.")
        print("Please run 'aws sso login --profile <profile>' first.")
        sys.exit(1)

    instances = []

    try:
        # Query instances with name filters
        response = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': TARGET_NAMES
                }
            ]
        )

        # Parse response
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                # Extract name from tags
                name = ''
                for tag in instance.get('Tags', []):
                    if tag['Key'] == 'Name':
                        name = tag['Value']
                        break

                instances.append({
                    'id': instance['InstanceId'],
                    'name': name,
                    'state': instance['State']['Name']
                })

        return instances

    except ClientError as e:
        print(f"Error querying EC2 instances: {e}")
        sys.exit(1)


def display_instance_menu(instances):
    """
    Display interactive checkbox menu to select one or more instances.
    Returns list of selected instance IDs.
    """
    if not instances:
        print("No instances found matching the target names.")
        print(f"Looking for: {', '.join(TARGET_NAMES)}")
        return []

    # Filter to only running instances
    running_instances = [i for i in instances if i['state'] == 'running']

    if not running_instances:
        print("No running instances found.")
        print(f"\nFound {len(instances)} stopped instance(s):")
        for inst in instances:
            print(f"  - {inst['name']} ({inst['id']}) - {inst['state']}")
        return []

    # Sort instances by type
    sorted_instances = sort_instances_by_type(running_instances)

    # Group instances by type for quick-select options
    workers = [i for i in sorted_instances if 'worker' in i['name']]
    schedulers = [i for i in sorted_instances if 'scheduler' in i['name']]
    triggerers = [i for i in sorted_instances if 'triggerer' in i['name']]
    webservers = [i for i in sorted_instances if 'webserver' in i['name']]

    # Build choices with quick-select options
    choices = []
    if workers:
        choices.append('[All Workers]')
    if schedulers:
        choices.append('[All Schedulers]')
    if triggerers:
        choices.append('[All Triggerers]')
    if webservers:
        choices.append('[All Webservers]')

    # Add individual instances (already sorted)
    for inst in sorted_instances:
        choices.append(f"{inst['name']} ({inst['id']})")

    # Add Exit option at the end
    choices.append('[Exit]')

    # Display checkbox menu
    questions = [
        inquirer.Checkbox(
            'instances',
            message="Select EC2 instances (Space=select, Enter=confirm)",
            choices=choices,
        ),
    ]

    answers = inquirer.prompt(questions)

    if not answers or not answers['instances']:
        return []

    # Process selections - expand quick-select options
    selected_items = answers['instances']

    # Check for Exit selection
    if '[Exit]' in selected_items:
        print("\nExiting.")
        return []

    # Remove Exit from selections
    selected_items = [item for item in selected_items if item != '[Exit]']

    if not selected_items:
        print("\nNo instances selected. Exiting.")
        return []

    instance_ids = []

    for item in selected_items:
        if item == '[All Workers]':
            instance_ids.extend([i['id'] for i in workers])
        elif item == '[All Schedulers]':
            instance_ids.extend([i['id'] for i in schedulers])
        elif item == '[All Triggerers]':
            instance_ids.extend([i['id'] for i in triggerers])
        elif item == '[All Webservers]':
            instance_ids.extend([i['id'] for i in webservers])
        elif item != '---':
            # Extract instance ID from format: "name (id)"
            instance_id = item.split('(')[1].split(')')[0]
            instance_ids.append(instance_id)

    # Remove duplicates while preserving order
    seen = set()
    unique_ids = []
    for iid in instance_ids:
        if iid not in seen:
            seen.add(iid)
            unique_ids.append(iid)

    return unique_ids


def open_iterm_split_pane(instance_id, instance_name, aws_profile=None):
    """
    Open iTerm split pane (horizontally - stacked) and connect using AWS SSM.
    instance_id: EC2 instance ID
    instance_name: EC2 instance name
    aws_profile: AWS profile name (defaults to 'motor-dev' if None)
    """
    # Default profile to motor-dev if not provided
    profile = aws_profile if aws_profile else 'motor-dev'

    # Build AWS SSM command with profile flag
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


def connect_to_instance(instance_id):
    """
    Connect to EC2 instance via AWS SSM Session Manager.
    """
    print(f"\nConnecting to {instance_id} via SSM Session Manager...")
    print("(Press Ctrl+D or type 'exit' to close the session)\n")

    try:
        # Start SSM session
        result = subprocess.run(
            ['aws', 'ssm', 'start-session', '--target', instance_id],
            check=True
        )

        if result.returncode == 0:
            print("\nSession closed.")

    except subprocess.CalledProcessError as e:
        print(f"\nError: Failed to start SSM session.")
        print("Possible causes:")
        print("  - SSM agent not installed/running on the instance")
        print("  - Instance not configured for Session Manager")
        print("  - Insufficient IAM permissions")
        print(f"\nError details: {e}")
        sys.exit(1)

    except FileNotFoundError:
        print("\nError: AWS CLI not found.")
        print("Please install AWS CLI: https://aws.amazon.com/cli/")
        sys.exit(1)


def connect_to_instances(instance_ids, instances, aws_profile=None):
    """
    Connect to instances.
    Single instance: connect in current pane.
    Multiple instances: create horizontal splits (stacked vertically).
    aws_profile: AWS profile name (defaults to 'motor-dev' if None)
    """
    if not instance_ids:
        print("\nNo instances selected. Exiting.")
        return

    # Use motor-dev as default profile if none provided
    profile = aws_profile if aws_profile else 'motor-dev'

    # Create a mapping of instance IDs to names
    id_to_name = {inst['id']: inst['name'] for inst in instances}

    if len(instance_ids) == 1:
        # Single instance - connect in current pane
        instance_id = instance_ids[0]
        instance_name = id_to_name.get(instance_id, 'Unknown')
        print(f"\nConnecting to {instance_name} ({instance_id}) in current pane...")

        ssm_command = f"aws ssm start-session --profile {profile} --target {instance_id}"

        try:
            subprocess.run(ssm_command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to connect to {instance_name}: {e}")
            sys.exit(1)
    else:
        # Multiple instances - create horizontal splits (stacked)
        print(f"\nUsing AWS profile: {profile}")
        print(f"Opening {len(instance_ids)} horizontal split panes...")

        for instance_id in instance_ids:
            instance_name = id_to_name.get(instance_id, 'Unknown')
            print(f"  - Opening split for {instance_name} ({instance_id})")
            open_iterm_split_pane(instance_id, instance_name, profile)
            time.sleep(0.3)  # Small delay between splits

        print(f"\nOpened {len(instance_ids)} split pane(s) successfully!")


def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='EC2 Instance Manager - Connect to Airflow instances via AWS SSM'
    )
    parser.add_argument(
        '--profile',
        type=str,
        help='AWS profile name to use for authentication (default: motor-dev)',
        default=None
    )
    args = parser.parse_args()

    print("EC2 Instance Manager - Airflow Instances\n")

    # Use motor-dev as default profile if none provided
    profile = args.profile if args.profile else 'motor-dev'

    # Ensure AWS SSO login
    aws_sso_login(profile)
    print()

    # Fetch instances
    print("Fetching EC2 instances...")
    instances = get_ec2_instances(profile)

    # Display menu and get selections
    instance_ids = display_instance_menu(instances)

    # Connect to selected instances
    connect_to_instances(instance_ids, instances, profile)


if __name__ == '__main__':
    main()
