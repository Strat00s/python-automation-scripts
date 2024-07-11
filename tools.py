import subprocess

class logger:
    def __init__(self):
        self.text = ""

    def add(self, y, echo = False):
        if echo:
            print(y, end="")
        self.text += y

    def get(self):
        return self.text

    def clear(self):
        self.text = ""

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