from fabric.api import *

env.hosts = ['tangh@server']
env.key_filename = '~/server'

def gather():
    get('~/time.txt', './time.txt')
    get('~/rtt.txt', './rtt.txt')

def extract():
    run("grep 'time:' server.log | awk '{print $3}' | head -n 1 > time.txt")
    run("grep 'time:' server.log | awk '{print $3}' | tail -n 1 >> time.txt")
    run("grep 'rtt' server.log| awk '{print $3}' > rtt.txt")


