import subprocess
import json

def run(cmd, token):
    print(cmd)
    cmd=cmd.replace('\n',' ')
    result = subprocess.run(["upsolver", "execute", "-t", token, "-c", cmd],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                           shell=True)

    if result.returncode == 0:
        data = result.stdout
        print(data)
    else:
        print(result.stderr)
        return False, result.stderr

    for line in data:
        if line.startswith('['):
            break
        else:
            continue

    buffer = ""
    event = ""
    table_result = []
    for line in data:
        # remove empty and "connection" lines (a comma)
        if not line.strip(', \n'):
            continue
        buffer += line

        try:
            event = json.loads(buffer)
        except json.decoder.JSONDecodeError:
            pass
        else:
            #print(event)
            table_result.append(event)
            buffer = ""
                #print(buffer)

    return True, table_result