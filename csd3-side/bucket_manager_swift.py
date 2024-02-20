#!/usr/env/python

"""
Helper functions for using an Openstack Swift Container
"""
import swiftclient
import json
import os

def print_containers(conn):
	for container in conn.get_account()[1]:
        	print(container['name'])

def print_contents(container_name):
    for data in conn.get_container(container_name)[1]:
        print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))

def get_conn(username, access_key, host):
	return swiftclient.Connection(
		user = username,
		key = access_key,
        authurl = host
	)
