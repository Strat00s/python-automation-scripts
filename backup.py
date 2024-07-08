import argparse
import yaml
import os
import stat
import subprocess
import datetime
import send_email


LOG_LEN = 20


def run_command(command: str)-> list:
    try:
        proc = subprocess.run(command.strip().split(" "), check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return [True, proc.stdout.decode("utf-8").strip()]
    except subprocess.CalledProcessError as e:
        return [False, e.output.decode("utf-8").strip(), e.returncode]


def run_backup(backup_name: str, config: dict, smtp: dict):
    paths = config["paths"]
    if isinstance(paths, str):
        paths = [paths]
    repo = config["repo"]
    email = config["email"]
    services = None
    if "services" in config.keys():
        services = config["services"]
    commands = None
    if "commands" in config.keys():
        commands = config["commands"]
        if isinstance(commands, str):
            commands = [commands]
    keep = None
    if "keep" in config.keys():
        keep = config["keep"]


    # 1. Send email about start
    body = f"Backup {backup_name} is starting.\n\n"
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

    body += f"  {step}. Change permissions for file(s) and folder(s):\n"
    for path in paths:
        body += f"    {path}\n"
    step += 1

    body += f"  {step}. Run borg backup.\n"
    step += 1

    body += f"  {step}. Restore permissions for file(s) and folder(s)\n"
    step += 1
    
    if services is not None:
        body += f"  {step}. Start all services\n"
        step += 1

    if keep is not None:
        body += f"  {step}. Prune a compact repository\n"

    if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} started", body):
        print("Failed to send email!\n")

    logs = "Starting...\n"

    attachments = dict()


    # 2. Run command
    if commands is not None:
        logs += "\n############################# Runing Commands #############################\n\n"
        for i, command in enumerate(commands):
            ret =  run_command(command)
            if ret[0]:
                logs += f'Running {i + 1}. command "{command}":\n'
                if ret[1].count("\n") <= LOG_LEN:
                    logs += f"{ret[1]}\n\n"
                attachments[f"cmd_{i + 1}.out"] = ret[1]
            else:
                logs += f'Failed to run "{command}":\n'
                logs += f"{ret[1]}\n"
                logs += f"Return code: {ret[2]}\n"
                logs += "Stopping now!\n"
                print(logs)
                if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                    print("Failed to send email!\n")
                return False


    # 3. Stop service
    if services is not None:
        logs += "\n############################ Stopping Services ############################\n\n"
        for i, service in enumerate(services):
            ret = False
            if services[service] == "system":
                ret = run_command(f"service {service} stop")
            elif services[service] == "docker":
                ret = run_command(f"docker stop {service}")
            else:
                logs += f'Unknown service type "{services[service]}"'
                logs += "Stopping now!\n"
                print(logs)
                if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                    print("Failed to send email!\n")
                return False
            
            if ret[0]:
                logs += f'Stopped service {service}:\n'
                if ret[1].count("\n") <= LOG_LEN:
                    logs += f"{ret[1]}\n\n"
                attachments[f"service_{i + 1}_stop.out"] = ret[1]
            else:
                logs += f'Failed to stop service {service}:\n'
                logs += f"{ret[1]}\n"
                logs += f"Return code: {ret[2]}\n"
                logs += "stopping now!\n"
                print(logs)
                if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                    print("Failed to send email!\n")
                return False


    # 4. Save current permissions
    # 5. Make paths read only
    logs += "\n########################### Changing Permissions ##########################\n\n"
    permissions = {}
    for path in paths:
        if os.path.isfile(path):
            permissions[path] = oct(os.stat(path).st_mode)[-3:]
            continue

        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                tmp_path = os.path.join(root, name)
                permissions[tmp_path] = oct(os.stat(path).st_mode)[-3:]
    
    with open("permissions.yaml", "w") as f:
        yaml.safe_dump(permissions, f)
        logs += f"Saved permissions to permissions.yaml\n\n"
    
    with open("permissions.yaml") as f:
        attachments["permissions.yaml"] = f.read()

    for path in permissions:
        os.chmod(path, stat.S_IREAD)

    logs += f"Changed permissions of {len(permissions)} to read-only.\n"

    # 6. Backup everything via borg (data, service, extra)
    logs += "\n############################### Runing borg ###############################\n\n"

    # check if repo exists and create it if it doesn't
    os.environ["BORG_PASSPHRASE"] = config["pass"]
    child = subprocess.run(["borg", "init", "--encryption", "repokey", repo], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if child.returncode == 0:
        logs += f'Repository "{repo}" created.\n\n'
        child = subprocess.run(["borg", "key", "export", repo], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # save passphrase
        if child.returncode == 0:
            attachments[f"{backup_name}.key"] = child.stdout.decode("utf-8").strip()
        else:
            logs += "Failed to save passphrase. Stopping now!\n"
            print(logs)
            if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                print("Failed to send email!\n")
            return False

    elif f"A repository already exists at {repo}." in child.stdout.decode("utf-8").strip():
        logs += f'Repository "{repo}" already exists.\n\n'
    
    archive_name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    command = f"borg create --stats --verbose --info --progress {repo}::{archive_name.replace(' ', '_')} {' '.join(paths)}"
    ret = run_command(command)
    attachments["borg.out"] = ret[1]
    results = ret[1][ret[1].find("------------------------------------------------------------------------------"):]

    if ret[0]:
        logs += f'Running "{command}":\n'
        logs += f"{results}\n"

    else:
        logs += f'Running "{command}" failed:\n'
        logs += f"Output: {results}\n"
        logs += f"Return code: {ret[2]}\n"
        logs += "stopping now!\n"
        print(logs)
        if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
            print("Failed to send email!\n")
        return False


    # 7. Restore permissions
    logs += "\n########################## Restoring Permissions ##########################\n\n"
    perm_change = ""
    for path in permissions:
        perm_change += f"{path}: {oct(os.stat(path).st_mode)[-3:]} -> {permissions[path]}\n"
        os.chmod(path, int(permissions[path], 8))
    
    logs += f"Restored permissions of {len(permissions)}.\n"

    # 8. Start service
    if services is not None:
        logs += "\n############################ Starting Services ############################\n\n"
        for i, service in enumerate(services):
            ret = False
            if services[service] == "system":
                ret = run_command(f"service {service} start")
            elif services[service] == "docker":
                ret = run_command(f"docker start {service}")
    
            if ret[0]:
                logs += f'Started service {service}:\n'
                if ret[1].count("\n") < LOG_LEN:
                    logs += f"{ret[1]}\n\n"
                attachments[f"service_{i + 1}_start.out"] = ret[1]
            else:
                logs += f'Failed to start service {service}:\n'
                logs += f"{ret[1]}\n"
                logs += f"Return code: {ret[2]}\n"
                logs += "Stopping now!\n"
                print(logs)
                if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                    print("Failed to send email!\n")
                return False


    # 9. Prune and compact
    if keep is not None:
        logs += "\n############################# Prune & Compact #############################\n\n"

        command = "borg prune --stats --verbose --info --progress --list "
        if isinstance(keep, list):
            keep = [int(x) for x in keep]
            if keep[0] > 0:
                command += f"--keep-daily {keep[0]} "
            if keep[1] > 0:
                command += f"--keep-weekly {keep[1]} "
            if keep[2] > 0:
                command += f"--keep-monthly {keep[2]} "
            if keep[3] > 0:
                command += f"--keep-yearly {keep[3]} "
        else:
            if isinstance(keep, str):
                keep = int(keep)
            command += f"--keep-last {keep} "

        command += repo

        ret =  run_command(command)
        if ret[0]:
            logs += f'Running "{command}":\n'
            if ret[1].count("\n") <= LOG_LEN:
                logs += f"{ret[1]}\n\n\n"
            attachments[f"borg_prune.out"] = ret[1]
        else:
            logs += f'Failed to run "{command}":\n'
            logs += f"{ret[1]}\n"
            logs += f"Return code: {ret[2]}\n"
            logs += "Stopping now!\n"
            print(logs)
            if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                print("Failed to send email!\n")
            return False

        command = f"borg compact --progress --verbose {repo}"
        ret =  run_command(command)
        if ret[0]:
            logs += f'Running "{command}":\n'
            if ret[1].count("\n") <= LOG_LEN:
                logs += f"{ret[1]}\n\n"
            attachments[f"borg_compact.out"] = ret[1]
        else:
            logs += f'Failed to run "{command}":\n'
            logs += f"{ret[1]}\n"
            logs += f"Return code: {ret[2]}\n"
            logs += "Stopping now!\n"
            print(logs)
            if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} failed", logs, attachments):
                print("Failed to send email!\n")
            return False


    # 10. Clean after myseld
    os.environ["BORG_PASSPHRASE"] = ""
    os.remove("permissions.yaml")
    #logs += "Removed permissions.yaml\n"


    # 11. Send email about report
    if not send_email.send(smtp, "DataServer - Borg", email, f"Backup {backup_name} finished", logs, attachments):
        print("Failed to send email!\n")
    return True



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Borg backup runner", description="Simple script for running borg backups")
    parser.add_argument("-c", "--config", required=True, help='backup configurations yaml file')
    parser.add_argument("-s", "--smtp",   required=True, help='smtp yaml configuration for sending emails ("server", "port", "sender_addr", "username", "password")')
    
    args = parser.parse_args()

    config = dict()
    smtp = dict()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    with open(args.smtp) as f:
        smtp = yaml.safe_load(f)

    for key in config:
        if run_backup(key, config[key], smtp) != True:
            print("Backup failed!")
            exit(1)