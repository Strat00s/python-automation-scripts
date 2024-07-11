import argparse
import yaml
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, parent_dir)

import tools
import send_email


LOG_LEN = 20
HEADER_WIDTH = 80


def run_scrub(smtp_cfg:dict|None = None, sender:str|None = None, recipient:str|None = None):
    log = tools.logger()
    attachments = dict()

    # get pools
    command = "zpool list -H -o name"

    log.add(f"{tools.create_header('Pool List', HEADER_WIDTH)}\n\n", True)
    log.add(f'Running command "{command}":\n', True)
    ret = tools.run_proccess(command, True)
    log.add(tools.shorten_text(ret[1], LOG_LEN).strip() + "\n")
    log.add(f"Return code: {ret[0]}\n\n", True)
    if ret[1].count("\n") > LOG_LEN:
        attachments[f"pools.txt"] = ret[1]

    # run scrub for each and wait for it to finish
    log.add(f"{tools.create_header('Scrub Status', HEADER_WIDTH)}\n\n", True)
    for pool in ret[1].strip().split("\n"):
        command = f"zpool scrub -w {pool}"
        log.add(f'Scrubbing "{pool}" - "{command}":\n', True)
        ret = tools.run_proccess(command, True)
        log.add(tools.shorten_text(ret[1], LOG_LEN).strip() + "\n")
        log.add(f"Return code: {ret[0]}\n\n", True)
        if ret[1].count("\n") > LOG_LEN:
            attachments[f"{pool}_result.txt"] = ret[1]


    if smtp_cfg is not None and recipient is not None:
        if not send_email.send(smtp_cfg, sender if sender is not None else smtp_cfg["sender_addr"], recipient, "ZFS Status", log.get(), attachments):
            print("Failed to send email!")

    return False if ret[0] else True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="ZFS status checker", description="Simple script for checking ZFS pool status")
    parser.add_argument("-r", "--recipient", help='Email recipient when using smtp')
    parser.add_argument("-s", "--sender", help='Sender name')
    parser.add_argument("-c", "--config", help='SMTP YAML configuration for sending emails ("server", "port", "sender_addr", "username", "password")')

    args = parser.parse_args()

    if (args.config is None and args.recipient is not None) or (args.config is not None and args.recipient is None):
        print("When sending an email, both -r/--recipient and -s/--smtp arguments are required!\n\n")
        parser.print_help()
        exit(0)

    smtp = None
    if args.config is not None:
        with open(args.config) as f:
            smtp = yaml.safe_load(f)


    if not run_scrub(smtp, args.sender, args.recipient):
        print("Failed to get status!")
        exit(1)