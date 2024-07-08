import argparse
import yaml
import os
import stat
import subprocess
import datetime
import send_email


LOG_LEN = 20

class logger:
    def __init__(self):
        self.text = ""

    def add(self, y, echo = False):
        if echo:
            print(y)
        self.text += y

    def get(self):
        return self.text

    def clear(self):
        self.text = ""


def run_command(command: str, echo = False)-> list:
    output = ""
    process = subprocess.Popen(command.strip().split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    if echo:
        while process.poll() is None:
            for line in process.stdout:
                print(line, end="")
                output += line
    else:
        process.wait()
        output = process.stdout.read()

    if process.returncode == 0:
        return [True, output, 0]
    else:
        return [False, output, process.returncode]
    


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
    body = f'Backup "{backup_name}" is starting.\n\n'
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

    body += f"  {step}. Run borg backup.\n"
    step += 1

    if services is not None:
        body += f"  {step}. Start all services.\n"
        step += 1

    if keep is not None:
        body += f"  {step}. Prune and compact repository.\n"

    if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" started', body):
        print("Failed to send email!\n")

    log = logger()

    log.add("Starting...\n", True)

    attachments = dict()


    # 2. Run command
    if commands is not None:
        log.add("\n############################# Runing Commands #############################\n\n", True)
        for i, command in enumerate(commands):
            ret =  run_command(command)
            if ret[0]:
                log.add(f'Running {i + 1}. command "{command}":\n', True)
                if ret[1].count("\n") <= LOG_LEN:
                    log.add(f"{ret[1]}\n\n", True)
                attachments[f"cmd_{i + 1}.txt"] = ret[1]
            else:
                log.add(f'Failed to run "{command}":\n', True)
                log.add(f"{ret[1]}\n", True)
                log.add(f"Return code: {ret[2]}\n", True)
                log.add("Stopping now!\n", True)
                if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
                    print("Failed to send email!\n")
                return False


    # 3. Stop service
    if services is not None:
        log.add("\n############################ Stopping Services ############################\n\n", True)
        for i, service in enumerate(services):
            ret = False
            if services[service] == "system":
                ret = run_command(f"service {service} stop")
            elif services[service] == "docker":
                ret = run_command(f"docker stop {service}")
            else:
                log.add(f'Unknown service type "{services[service]}"', True)
                log.add("Stopping now!\n", True)
                
                if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
                    print("Failed to send email!\n")
                return False
            
            if ret[0]:
                log.add(f'Stopped service {service}:\n', True)
                if ret[1].count("\n") <= LOG_LEN:
                    log.add(f"{ret[1]}\n\n", True)
                attachments[f"service_{i + 1}_stop.txt"] = ret[1]
            else:
                log.add(f'Failed to stop service {service}:\n', True)
                log.add(f"{ret[1]}\n", True)
                log.add(f"Return code: {ret[2]}\n", True)
                log.add("Stopping now!\n", True)
                
                if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
                    print("Failed to send email!\n")
                return False


    # 4. Backup everything via borg (data, service, extra)
    log.add("\n############################### Runing borg ###############################\n\n", True)

    # check if repo exists and create it if it doesn't
    os.environ["BORG_PASSPHRASE"] = config["pass"]
    ret = run_command(f"borg init --encryption repokey {repo}", True)
    if ret[2] == 0:
        log.add(f'Repository "{repo}" created.\n\n', True)
        ret = run_command(f"borg key export {repo}", True)
        # save passphrase
        if ret[2] == 0:
            attachments[f"{backup_name}.key"] = ret[1]
        else:
            log.add("Failed to save passphrase. Stopping now!\n", True)
            
            if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
                print("Failed to send email!\n")
            return False

    elif f"A repository already exists at {repo}." in ret[1]:
        log.add(f'Repository "{repo}" already exists.\n\n', True)
    
    archive_name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    command = f"borg create --stats --verbose --info --progress {repo}::{archive_name.replace(' ', '_')} {' '.join(paths)}"
    ret = run_command(command, True)
    attachments["borg.txt"] = ret[1]
    results = ret[1][ret[1].find("------------------------------------------------------------------------------"):]

    if ret[0]:
        log.add(f'Running "{command}":\n')
        log.add(f"{results}\n")

    else:
        log.add(f'Running "{command}" failed:\n')
        log.add(f"Output: {results}\n")
        log.add(f"Return code: {ret[2]}\n", True)
        log.add("Stopping now!\n", True)
        
        if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
            print("Failed to send email!\n")
        return False


    # 5. Start service
    if services is not None:
        log.add("\n############################ Starting Services ############################\n\n", True)
        for i, service in enumerate(services):
            ret = False
            if services[service] == "system":
                ret = run_command(f"service {service} start")
            elif services[service] == "docker":
                ret = run_command(f"docker start {service}")
    
            if ret[0]:
                log.add(f'Started service {service}:\n', True)
                if ret[1].count("\n") < LOG_LEN:
                    log.add(f"{ret[1]}\n\n", True)
                attachments[f"service_{i + 1}_start.txt"] = ret[1]
            else:
                log.add(f'Failed to start service {service}:\n', True)
                log.add(f"{ret[1]}\n", True)
                log.add(f"Return code: {ret[2]}\n", True)
                log.add("Stopping now!\n", True)
                
                if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" failed', log.get(), attachments):
                    print("Failed to send email!\n")
                return False


    # 7. Prune and compact
    if keep is not None:
        log.add("\n\n\nPrune and compact results will be sent in next email.\n\n\n", True)

        # 8. Send email about report
        if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" finished', log.get(), attachments):
            print("Failed to send email!\n")

        attachments = dict()
        log.clear()
        log.add("\n############################# Prune & Compact #############################\n\n", True)

        command = "borg prune --stats --verbose --info --progress "
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

        ret =  run_command(command, True)
        if ret[0]:
            log.add(f'Running "{command}":\n')
            if ret[1].count("\n") <= LOG_LEN:
                log.add(f"{ret[1]}\n\n\n")
            attachments[f"borg_prune.txt"] = ret[1]
        else:
            log.add(f'Failed to run "{command}":\n')
            log.add(f"{ret[1]}\n")
            log.add(f"Return code: {ret[2]}\n", True)
            log.add("Stopping now!\n", True)
            
            if not send_email.send(smtp, "DataServer - Borg prune", email, f'Pruning "{backup_name}" failed', log.get(), attachments):
                print("Failed to send email!\n")
            return False

        command = f"borg compact --progress --verbose {repo}"
        ret =  run_command(command, True)
        if ret[0]:
            log.add(f'Running "{command}":\n')
            if ret[1].count("\n") <= LOG_LEN:
                log.add(f"{ret[1]}\n\n")
            attachments[f"borg_compact.txt"] = ret[1]
        else:
            log.add(f'Failed to run "{command}":\n')
            log.add(f"{ret[1]}\n")
            log.add(f"Return code: {ret[2]}\n", True)
            log.add("Stopping now!\n", True)
            
            if not send_email.send(smtp, "DataServer - Borg compact", email, f'Compact "{backup_name}" failed', log.get(), attachments):
                print("Failed to send email!\n")
            return False
        

        # 9. Send email about prune 
        if not send_email.send(smtp, "DataServer - Borg prune & compact", email, f'Prune & compact "{backup_name}" finished', log.get(), attachments):
            print("Failed to send email!\n")
    else:
        # 8. Send email about report
        if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" finished', log.get(), attachments):
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
        ret = run_backup(key, config[key], smtp)
        os.environ["BORG_PASSPHRASE"] = ""
        if ret != True:
            print("Backup failed!")
            exit(1)