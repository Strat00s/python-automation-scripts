import argparse
import yaml
import os
import subprocess
import send_email


def run_command(command: str)-> list:
    try:
        proc = subprocess.run(command.strip().split(" "), check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return [True, proc.stdout]
    except subprocess.CalledProcessError as e:
        return [False, e.output, e.returncode]


def run_backup(name: str, config: dict, smtp: dict):
    print(f"Running {name}")
    
    paths = config["paths"]
    if isinstance(paths, str):
        paths = [paths]
    
    repo = config["repo"]
    
    keep = config["keep"]
    if isinstance(keep, list):
        for i in range(0, len(keep)):
            if isinstance(keep[i], str):
                keep[i] = int(keep)
    elif isinstance(keep, str):
        keep = int(keep)
    
    email = config["email"]
    
    services = None
    if "services" in config.keys():
        services = config["services"]
    
    commands = None
    if "commands" in config.keys():
        commands = config["commands"]
        if isinstance(commands, str):
            commands = [commands]


    # 1. Send email about start

    body = f"Backup {name} is starting.\n\n"
    body += "The following steps will be executed:\n"
    step = 1

    if commands is not None:
        body += f"  {step}. Run command(s):\n"
        for command in commands:
            body += f"    {command}\n"
        step += 1

    if services is not None:
        body += f"  {step}. Stop service(s):\n"
        for service in services:
            body += f"    {service}\n"
        step += 1

    body += f"  {step}. Change permission for file(s) and folder(s):\n"
    for path in paths:
        body += f"    {path}\n"
    step += 1

    body += f"  {step}. Run borg backup.\n"
    step += 1

    body += f"  {step}. Restore permissions for file(s) and folder(s)\n"
    step += 1
    
    if services is not None:
        body += f"  {step}. Start all services\n"

    #TODO uncomment
    #send_email.send(smtp, "DataServer - Borg", email, f"Backup {name} started", body)
    print(body)

    logs = "Starting...\n"
    logs += "\n########################################################################\n\n"


    # 2. Run command
    if commands is not None:
        for command in commands:
            ret =  run_command(command)
            logs += f"Running command: {command}\n"
            if ret[0]:
                logs += "Output:\n"
                logs += f"{ret[1]}\n\n"
            else:
                logs += "Output:\n"
                logs += f"{ret[1]}\n"
                logs += f"Return code: {ret[2]}\n"
                logs += "Stopping now."
                print(logs)
                #send_email.send(smtp, "DataServer - Borg", email, f"Backup {name} failed", logs)
                return False
        logs += "########################################################################\n\n"

    print(logs)

    # 3. Stop service
    # 4. Save current permission
    # 5. Make paths read only
    # 6. Backup everything via borg (data, service, extra)
    # 7. Restore permissions
    # 8. Start service
    # 9. Send email about report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Borg backup runner", description="Simple script for running borg backups")
    parser.add_argument("-c", "--config", required=True, help='backup configurations yaml file')
    parser.add_argument("-s", "--smtp",   required=True, help='smtp yaml configuration for sending emails ("server", "port", "sender_addr", "username", "password")')
    
    args = parser.parse_args()

    config = dict()
    smtp = dict()
    with open(args.config) as f:
        config = yaml.safe_load(f)
        print(config)
    
    with open(args.smtp) as f:
        smtp = yaml.safe_load(f)
        print(smtp)
    
    for key in config:
        run_backup(key, config[key], smtp)