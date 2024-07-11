import argparse
import yaml
import os
import stat
import subprocess
import datetime
import send_email


LOG_LEN = 20
HEADER_WIDTH = 80

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

#TODO fix it everywhere; changed from [bool, str, int] to [int, str]
def run_proccess(command:str, echo = False) -> tuple[int, str]:
    output = ""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=True)
    
    if echo:
        while process.poll() is None:
            for line in process.stdout:
                print(line, end="")
                output += line
    else:
        process.wait()
        output = process.stdout.read()

    return (process.returncode, output)


def create_header(message:str, width:int) -> str:
    padding = (width - len(message) - 2) // 2
    header = f"{'#' * padding} {message} {'#' * padding}"
    if len(header) < width:
        header += "#"
    return header


def shorten_text(text:str, max_lines:int) -> str:
    lines = text.split('\n')
    if len(lines) > max_lines:
        half_lines = max_lines // 2
        start_lines = lines[:half_lines]
        end_lines = lines[-half_lines:]
        return '\n'.join(start_lines) + '\n...\n' + '\n'.join(end_lines)
    return text


def run_commands(commands:list, pos:str, log:logger) -> dict:
    attachments = dict()
    log.add(f"\n{create_header('Runing Commands', HEADER_WIDTH)}\n\n", True)
    for i, command in enumerate(commands):
        log.add(f'Running {i + 1}. command "{command}":\n', True)
        ret = run_proccess(command, True)
        log.add(shorten_text(ret[1], LOG_LEN).strip() + "\n")
        log.add(f"Return code: {ret[0]}\n\n", True)
        if ret[1].count("\n") > LOG_LEN:
            attachments[f"{pos}_cmd_{i + 1}.txt"] = ret[1]
    return attachments


def stop_start_service(services:dict, start:bool, log:logger) -> dict:
    attachments = dict()
    log.add(f"\n{create_header('Starting Services' if start else 'Stopping Services', HEADER_WIDTH)}\n\n", True)
    for key in services:
        if key not in ["system", "docker"]:
            continue

        start_stop = "start" if start else "stop"

        for service in services[key]:
            log.add(f'Stopping service {service}:\n', True)
            if key == "system":
                ret = run_proccess(f"service {service} {start_stop}", True)
            if key == "docker":
                ret = run_proccess(f"docker {start_stop} {service}", True)

            log.add(shorten_text(ret[1], LOG_LEN).strip() + "\n")
            log.add(f"Return code: {ret[0]}\n\n", True)
            if ret[1].count("\n") > LOG_LEN:
                attachments[f"{service}_{start_stop}.txt"] = ret[1]
    return attachments


def run_backup(backup_name: str, config: dict, smtp: dict):
    paths = config["paths"]
    if isinstance(paths, str):
        paths = [paths]
    repo = config["repo"]
    email = config["email"]

    keep = config.get("keep", None)
    if isinstance(keep, str):
        keep = int(keep)
    elif isinstance(keep, list):
        keep = [int(x) for x in keep]

    commands = {k: v if isinstance(v, list) else [v] for k, v in config.get("commands", {}).items()}
    services = {k: v if isinstance(v, list) else [v] for k, v in config.get("services", {}).items()}


    # 1. Send email about start
    body = f'Backup "{backup_name}" is starting.\n\n'
    body += "The following steps will be executed:\n"
    step = 1

    if commands is not None and "pre_stop" in commands.keys():
        body += f"  {step}. Run command(s):\n"
        for command in commands["pre_stop"]:
            body += f"    {command}\n"
        step += 1

    if services is not None:
        body += f"  {step}. Stop service(s):\n"
        for key in services:
            for service in services[key]:
                body += f"    {service} ({key})\n"
        step += 1
        step += 1

    if commands is not None and "post_stop" in commands.keys():
        body += f"  {step}. Run command(s):\n"
        for command in commands["post_stop"]:
            body += f"    {command}\n"
        step += 1

    body += f"  {step}. Run borg backup.\n"
    step += 1

    if commands is not None and "pre_start" in commands.keys():
        body += f"  {step}. Run command(s):\n"
        for command in commands["pre_start"]:
            body += f"    {command}\n"
        step += 1

    if services is not None:
        body += f"  {step}. Start all services.\n"
        step += 1
    
    if commands is not None and "post_start" in commands.keys():
        body += f"  {step}. Run command(s):\n"
        for command in commands["post_start"]:
            body += f"    {command}\n"
        step += 1

    if keep is not None:
        body += f"  {step}. Prune and compact repository.\n"

    if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" started', body):
        print("Failed to send email!\n")


    log = logger()
    log.add("Starting...\n", True)
    attachments = dict()


    # Run pre stop commands
    if commands is not None and "pre_stop" in commands.keys():
        attachments.update(run_commands(commands["pre_stop"], "pre_stop", log))


    # Stop services
    if services is not None:
        attachments.update(stop_start_service(services, False, log))


    # Run post stop commands
    if commands is not None and "post_stop" in commands.keys():
        attachments.update(run_commands(commands["post_stop"], "post_stop", log))


    # Backup everything via borg (data, service, extra)
    log.add(f"\n{create_header('Runing Borg', HEADER_WIDTH)}\n\n", True)

    failed = False

    # check if repo exists and create it if it doesn't
    os.environ["BORG_PASSPHRASE"] = config["pass"]
    ret = run_proccess(f"borg init --encryption repokey-blake2 {repo}", True)
    if ret[0] == 0:
        log.add(f'Repository "{repo}" created.\n\n', True)
        ret = run_proccess(f"borg key export {repo}", False)
        # save passphrase
        if ret[0] == 0:
            attachments[f"{backup_name}.key"] = ret[1]
        else:
            failed = True
            log.add("Failed to get passphrase. Skipping backup!\n", True)

    elif f"A repository already exists at {repo}." in ret[1]:
        log.add(f'Repository "{repo}" already exists.\n\n', True)
    else:
        failed = True
        log.add("Failed to init or check repository. Skipping backup!\n", True)

    if not failed:
        archive_name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        command = f"borg create --stats --verbose --info --progress {repo}::{archive_name.replace(' ', '_')} {' '.join(paths)}"
        log.add(f'Running "{command}":\n')
        ret = run_proccess(command, True)
        results = ret[1][ret[1].find("------------------------------------------------------------------------------"):]
        max_len = 20*1000*1000
        if len(ret[1]) > max_len:
            half_length = max_len // 2
            start = ret[1][:half_length]
            end = ret[1][-half_length:]
            ret[1] = f"{start}...{end}"
        attachments["borg.txt"] = ret[1]

        log.add(f"{results}\n")
        log.add(f"Return code: {ret[0]}\n\n", True)
        
        if ret[0] != 0:
            log.add("Backup failed!\n")
            failed = True


    # Run pre start commands
    if not failed and commands is not None and "pre_start" in commands.keys():
        attachments.update(run_commands(commands["pre_start"], "pre_start", log))


    # Start services
    if services is not None:
        attachments.update(stop_start_service(services, True, log))


    # Run post start commands
    if not failed and commands is not None and "post_start" in commands.keys():
        attachments.update(run_commands(commands["post_start"], "post_start", log))


    # 7. Prune and compact
    if not failed and keep is not None:
        log.add(f"{create_header('Prune & Compact', HEADER_WIDTH)}\n\n", True)

        command = "borg prune --stats --verbose --info --progress "
        if isinstance(keep, list):
            if keep[0] > 0:
                command += f"--keep-daily {keep[0]} "
            if keep[1] > 0:
                command += f"--keep-weekly {keep[1]} "
            if keep[2] > 0:
                command += f"--keep-monthly {keep[2]} "
            if keep[3] > 0:
                command += f"--keep-yearly {keep[3]} "
        else:
            command += f"--keep-last {keep} "
        command += repo

        log.add(f'Running "{command}":\n')
        ret =  run_proccess(command, True)
        log.add(shorten_text(ret[1], LOG_LEN).strip() + "\n")
        log.add(f"Return code: {ret[0]}\n\n", True)
        if ret[1].count("\n") > LOG_LEN:
            attachments[f"borg_prune.txt"] = ret[1]

        command = f"borg compact --progress --verbose {repo}"
        log.add(f'Running "{command}":\n')
        ret =  run_proccess(command, True)
        log.add(shorten_text(ret[1], LOG_LEN).strip() + "\n")
        log.add(f"Return code: {ret[0]}\n\n", True)
        if ret[1].count("\n") > LOG_LEN:
            attachments[f"borg_compact.txt"] = ret[1]


    # 8. Send email about report
    if not send_email.send(smtp, "DataServer - Borg backup", email, f'Backup "{backup_name}" {"failed" if failed else "finished"}', log.get(), attachments):
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