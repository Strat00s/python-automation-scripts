import argparse
import smtplib
import ssl
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send(config: dict, sender_name: str, to_email: str, subject: str, body: str):
    print(config, sender_name, to_email, subject, body)

    for key in config:
        if key not in ["server", "port", "sender_addr", "username", "password"]:
            print(f"Configuration option {key} is invalid.")
            print('Only valid options are: "server", "port", "sender_addr", "username", "password".')
            return False

    if isinstance(config["port"], str):
        config["port"] = int(config["port"])


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"]    = f"{sender_name} <{config['sender_addr']}>"
    message["To"]      = to_email
    message["Subject"] = subject

    # Add body to_email the email
    message.attach(MIMEText(body, "plain"))

    # Create a secure SSL context
    context = ssl.create_default_context()

    try:
        # Connect to the SMTP server using SSL
        with smtplib.SMTP_SSL(config["server"], config["port"], context=context) as server:
            # Login to_email the SMTP server
            server.login(config["username"], config["password"])
            # Send email
            server.sendmail(config["sender_addr"], to_email, message.as_string())
        print("Email sent successfully")

    except Exception as e:
        print(f"Error occurred: {e}")
        return False
    
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="SMTP Email sender_name", description="Simple program for sending emails via SMTP")
    parser.add_argument("-c", "--config",          required=True, help='json file containing configuration ("server", "port", "sender_addr", "username", "password")')
    parser.add_argument("-s", "--sender_name",     required=True, help="sender_name name")
    parser.add_argument("-r", "--recipient_email", required=True, help="Recipient email")
    parser.add_argument("-S", "--subject",         required=True, help="Email subject")
    parser.add_argument("-b", "--body",            required=True, help="Text to_email be sent")

    args = parser.parse_args()

    config = dict()
    with open(args.config) as f:
        config = json.load(f)
    send(config, args.sender_name, args.recipient_email, args.subject, args.body)

