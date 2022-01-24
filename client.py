# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""An example Flight CLI client."""

import argparse
import sys

import pyarrow
import pyarrow.flight
import pyarrow.csv as csv
import datetime
import pandas

def list_flights(client, connection_args={}):
    print('Flights\n=======')
    path_list = []
    for flight in client.list_flights():
        descriptor = flight.descriptor
        if descriptor.descriptor_type == pyarrow.flight.DescriptorType.PATH:
            path_list.append(descriptor.path[0].decode())
        elif descriptor.descriptor_type == pyarrow.flight.DescriptorType.CMD:
            print("Command:", descriptor.command)
        else:
            print("Unknown descriptor type")

    return path_list

def get_flight(args, client, connection_args={}):
    if args.path:
        descriptor = pyarrow.flight.FlightDescriptor.for_path(*args.path)
    else:
        descriptor = pyarrow.flight.FlightDescriptor.for_command(args.command)

    info = client.get_flight_info(descriptor)
    for endpoint in info.endpoints:
        print('Ticket:', endpoint.ticket)
        for location in endpoint.locations:
            print(location)
            get_client = pyarrow.flight.FlightClient(location,
                                                     **connection_args)
            reader = get_client.do_get(endpoint.ticket)
            df = reader.read_pandas()
            print(df)

def select_path(path_list):
    m = datetime.timedelta(hours=9, minutes=1)
    now = datetime.datetime.now()
    be_minutes = now - m
    minutes_path = []
    for path in path_list:
        dt = datetime.datetime.strptime(path, '%Y-%m-%d-%H_%M_%S_%f')
        if be_minutes < dt:
            print(path)
            minutes_path.append(path)
    return minutes_path

def get_table(client, minutes_path, connection_args={}):
    tables = []
    for path in minutes_path:
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        info = client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            for location in endpoint.locations:
                get_client = pyarrow.flight.FlightClient(location, **connection_args)
                reader = get_client.do_get(endpoint.ticket)
                df = reader.read_pandas()
                tables.append(df)
    return pandas.concat(tables)

def write(host_name, table):
    write_list = []
    for i in range(2, 14):
        for row in table.iloc[:,[0,1,i]].itertuples():
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, table.columns[i], row[1], row[2], row[3]))
    for row in table[(table['Function'] == 1)|(table['Function'] == 2)].iloc[:,[0, 1, 14, 15, 16, 17]].itertuples():
        if row[8] == 502:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Coil", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoilData", row[1], row[2], row[4]))
        else:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoidMultCount", row[1], row[2], row[5]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoilMultData", row[1], row[2], row[6]))
    for row in table[(table['Function'] == 3)|(table['Function'] == 4)].iloc[:,[0, 1, 14, 15, 16, 17]].itertuples():
        if row[8] == 502:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Register", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterData", row[1], row[2], row[4]))
        else:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterMultCount", row[1], row[2], row[5]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterMultData", row[1], row[2], row[6]))
    for row in table[table['Function'] == 5].iloc[:,[0, 1, 14, 15]].itertuples():
        write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Coil", row[1], row[2], row[3]))
        write_list.append('{} {} {} {:0>9} {:016b}\n'.format(host_name, "CoilData", row[1], row[2], row[4]))
    for row in table[table['Function'] == 6].iloc[:,[0, 1, 14, 15]].itertuples():
        write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Register", row[1], row[2], row[3]))
        write_list.append('{} {} {} {:0>9} {:016b}\n'.format(host_name, "RegisterData", row[1], row[2], row[4]))
    for row in table[table['Function'] == 15].iloc[:,[0, 1, 14, 15, 16, 17]].itertuples():
        if row[8] == 502:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Coil", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoilData", row[1], row[2], row[4]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoidMultCount", row[1], row[2], row[5]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoilMultData", row[1], row[2], row[6]))
        else:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Coil", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "CoilData", row[1], row[2], row[4]))
    for row in table[table['Function'] == 16].iloc[:,[0, 1, 14, 15, 16, 17]].itertuples():
        if row[8] == 502:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Register", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterData", row[1], row[2], row[4]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterMultCount", row[1], row[2], row[5]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterMultData", row[1], row[2], row[6]))
        else:
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "Register", row[1], row[2], row[3]))
            write_list.append('{} {} {} {:0>9} {}\n'.format(host_name, "RegisterData", row[1], row[2], row[4]))
    
    with open("data/data.txt", "w") as f:
        f.write(''.join(write_list))


def main():

    
    host = 'localhost'
    port = 5005
    scheme = "grpc+tcp"
    connection_args = {}
    client = pyarrow.flight.FlightClient(f"{scheme}://{host}:{port}",
                                         **connection_args)
    while True:
        try:
            action = pyarrow.flight.Action("healthcheck", b"")
            options = pyarrow.flight.FlightCallOptions(timeout=1)
            list(client.do_action(action, options=options))
            break
        except pyarrow.ArrowIOError as e:
            if "Deadline" in str(e):
                print("Server is not ready, waiting...")
    
    path_list = list_flights(client, connection_args)
    minutes_path = select_path(path_list)
    table = get_table(client, minutes_path, conection_args)
    #table = get_table(client, path_list, connection_args)
    
    write("plc1", table)


if __name__ == '__main__':
    main()
