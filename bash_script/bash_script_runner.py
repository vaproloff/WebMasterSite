import subprocess


def run_bash_script():
    subprocess.run(["search_competitors.sh"], shell=True)
