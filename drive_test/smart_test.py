import argparse
import yaml
import sys
import os
import json
import datetime
import time
import math

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, parent_dir)

import tools
import send_email


LOG_LEN = 20
HEADER_WIDTH = 80




def run_test(extended:bool, smtp_cfg:dict|None = None, sender:str|None = None, recipient:str|None = None):
    log = tools.logger()
    attachments = dict()

    # get drives
    ret = tools.run_proccess("ls /dev/sd?")

    # check if they support smart
    log.add(f"{tools.create_header('Drive Capabilities', HEADER_WIDTH)}\n\n", True)
    drives = dict()
    timer = datetime.datetime.now()
    for drive in ret[1].strip().split("\n"):
        ret = tools.run_proccess(f"smartctl -c --json {drive}")
        smart_stats = json.loads(ret[1])
        exit_code = smart_stats["smartctl"]['exit_status']
        log.add(f"Drive {drive}:\n", True)
        log.add("S.M.A.R.T.:    ", True)
        if exit_code:
            log.add("NO\n", True)
            continue
        log.add("YES\n", True)
        times = smart_stats["ata_smart_data"]["self_test"]["polling_minutes"]
        test_type = "extended" if extended else "short"
        log.add(f'Short test:    {times["short"]} minute(s)\n', True)
        log.add(f'Extended test: {times["extended"]} minute(s)\n', True)
        
        drives[drive] = {"time":times[test_type], "prog": 0, "remain": times[test_type], "failed": 0, "status": "RUNNING"}
        log.add("\n", True)

    # start smart
    log.add(f"{tools.create_header('S.M.A.R.T. Test Started', HEADER_WIDTH)}\n\n", True)
    for drive, data in drives.items():
        ret = tools.run_proccess(f"smartctl -t {'long' if test_type == 'extended' else 'short'} {drive}")
        if not ret[0]:
            log.add(f"{drive}: {data['time']} minute(s) remaining ({test_type})\n", True)
        else:
            log.add(f"Failed to start test on {drive}:\n", True)
            log.add(tools.shorten_text(ret[1], LOG_LEN).strip() + "\n", True)
            log.add(f"Return code: {ret[0]}\n\n", True)
            if ret[1].count("\n") > LOG_LEN:
                attachments[f"pools.txt"] = ret[1]

    if smtp_cfg is not None and recipient is not None:
        if not send_email.send(smtp_cfg, sender if sender is not None else smtp_cfg["sender_addr"], recipient, "S.M.A.R.T. test started", log.get(), attachments):
            print("Failed to send email!")


    # monitor progress and send results
    log.clear()
    attachments = dict()

    sleep_t = 60
    if min([drives[x]['time'] for x in drives.keys()]) > 10:
        sleep_t = 10 * 60
    
    elif min([drives[x]['time'] for x in drives.keys()]) <= 1:
        sleep_t = 20

    print("\n  DRIVE  | PROGRESS | REMAINING | FAILED | STATUS")

    while True:
        remove = []
        for drive in drives:
            ret = tools.run_proccess(f"smartctl -c --json {drive}")
            if ret[0]:
                drives[drive]["failed"] += 1
                drives[drive]["prog"] = 100
                drives[drive]["remain"] = 0
                drives[drive]["status"] = "FAILED TO GET STATS"
            else:
                status = json.loads(ret[1])["ata_smart_data"]["self_test"]["status"]
                if "remaining_percent" in status.keys():
                    drives[drive]["prog"] = 100 - status["remaining_percent"]
                    drives[drive]["remain"] =  math.ceil(drives[drive]["time"] / 100 * status["remaining_percent"])
                else:
                    drives[drive]["prog"] = 100
                    drives[drive]["remain"] = 0
                    drives[drive]["status"] = "PASSED" if status["passed"] else "FAILED"

            print(f"{drive} |   {drives[drive]['prog']: >3d}%   | {drives[drive]['remain']: >3d} min   |   {drives[drive]['failed']}    | {drives[drive]['status']}\n", end="")

        if sum([drives[x]["remain"] for x in drives.keys()]) == 0 or all([drives[x]["prog"] == 100 for x in drives]):
            break

        time.sleep(sleep_t)
        
        #clear the screen
        for drive in drives:
            sys.stdout.write("\x1b[1A")  # Move up one line
            sys.stdout.write("\x1b[2K")  # Clear the line
            sys.stdout.flush()  # Flush the output buffer

    print("\n")

    log.add(f"{tools.create_header('Test Results', HEADER_WIDTH)}\n\n", True)
    for drive in drives:
        log.add(f"{drive}: {drives[drive]['status']}\n", True)
        ret = tools.run_proccess(f"smartctl -a {drive}")
        attachments[f"{drive.strip('/').replace('/', '-')}_results.txt"] = ret[1]

    if smtp_cfg is not None and recipient is not None:
        if not send_email.send(smtp_cfg, sender if sender is not None else smtp_cfg["sender_addr"], recipient, "ZFS Status", log.get(), attachments):
            print("Failed to send email!")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="ZFS status checker", description="Simple script for checking ZFS pool status")
    parser.add_argument("-r", "--recipient", help='Email recipient when using smtp')
    parser.add_argument("-s", "--sender", help='Sender name')
    parser.add_argument("-c", "--config", help='SMTP YAML configuration for sending emails ("server", "port", "sender_addr", "username", "password")')
    parser.add_argument("-e", "--extended", help="Run extended SMART test instead of short", action="store_true")

    args = parser.parse_args()

    if (args.config is None and args.recipient is not None) or (args.config is not None and args.recipient is None):
        print("When sending an email, both -r/--recipient and -s/--smtp arguments are required!\n\n")
        parser.print_help()
        exit(0)

    smtp = None
    if args.config is not None:
        with open(args.config) as f:
            smtp = yaml.safe_load(f)


    if not run_test(args.extended, smtp, args.sender, args.recipient):
        print("Failed to get status!")
        exit(1)
