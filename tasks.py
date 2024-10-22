
from invoke import task

@task
def build_package(c):
    c.run("python3 -m build")
