# Copyright 2020 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import sys
import glob
import time
import datetime

from rcl_interfaces.msg import Log
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import rosbag2_py  # noqa

argvs = sys.argv
argc = len(argvs)
filter = None


def get_rosbag_options(path, serialization_format='cdr'):
  storage_options = rosbag2_py.StorageOptions(uri=path, storage_id='sqlite3')

  converter_options = rosbag2_py.ConverterOptions(
      input_serialization_format=serialization_format,
      output_serialization_format=serialization_format)

  return storage_options, converter_options


interval = {}
intervals = {}

def write_topic(csv_f, topic_type, topic, data, ts):
  global interval
  global intervals

  if topic != '/diagnostics' and topic != '/system/emergency/hazard_status':
    return

  # Create timestamp
  date = '\'{}.{:0>9}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(ts / 1000 / 1000 / 1000)), ts % (1000 * 1000 * 1000))

  # Deserialize topic
  msg_type = get_message(topic_type)
  try:
    msg = deserialize_message(data, msg_type)
  except Exception as e:
    print(e)
    return

  if topic == '/diagnostics':
    for status in msg.status:
      # Filter by name
      if filter is not None:
        if status.name.startswith(filter) == False:
          continue

      level = int.from_bytes(status.level, 'little')
      key_value = ""
      # Append diagnostic array
      for value in status.values:
        key_value += '{},{},'.format(value.key, value.value)

      # If the topic already read
      if status.name in interval:
        # Write interval
        diff = (ts - interval[status.name]) / 1000 / 1000 / 1000
        csv_f.write('{:.03f},'.format(diff))
        csv_f.write('{},{},{},{},{}\n'.format(date, status.name, level, status.message, key_value))
        intervals[status.name].append(diff)

      else:
        csv_f.write(',{},{},{},{},{}\n'.format(date, status.name, level, status.message, key_value))
        interval[status.name] = 0
        intervals[status.name] = []

      interval[status.name] = ts

  elif topic == '/system/emergency/hazard_status':
    sf_value = ""
    lf_value = ""
    spf_value = ""
    if topic_type == 'autoware_auto_system_msgs/msg/HazardStatusStamped':
      #DiagStatus for Autoware.universe
      for ds in msg.status.diag_safe_fault:
          sf_value  += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)
      for ds in msg.status.diag_latent_fault:
          lf_value  += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)
      for ds in msg.status.diag_single_point_fault:
          spf_value += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)
    else:
      #DiagStatus for Autoware.IV
      for ds in msg.status.diagnostics_sf:
          sf_value  += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)
      for ds in msg.status.diagnostics_lf:
          lf_value  += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)
      for ds in msg.status.diagnostics_spf:
          spf_value += '{:d}:{}:{}|'.format(int.from_bytes(ds.level,'little'), ds.name, ds.message)

    csv_f.write(',{},{},{},SF={},LF={},SPF={}\n'.format(date, topic, msg.status.level, sf_value, lf_value, spf_value))

  else:
    csv_f.write('\n')


def process(source):

  path = datetime.datetime.now().strftime('rosbag_%Y%m%d-%H%M%S')
  if filter is not None:
    path = path + "_" + filter
  path = path + '.csv'

  path_interval = datetime.datetime.now().strftime('interval_%Y%m%d-%H%M%S.csv')

  # Generate temporary file
  with open("tmp.csv", mode='w') as wr_f:

    start = 0
    ts = 0
    files = []

    if os.path.splitext(source)[1] == ".db3":
      bag_paths  = [source]
    else:
      bag_paths = sorted(
          glob.glob(source + '/*.db3'), key=os.path.getmtime, reverse=False)

    for bag_path in bag_paths:
      files.append(os.path.basename(bag_path))

      storage_options, converter_options = get_rosbag_options(bag_path)
      reader = rosbag2_py.SequentialReader()
      try:
        reader.open(storage_options, converter_options)
      except Exception as e:
        print(e)
        continue
      print('rosbag2 Reader opened. ', end="")

      topic_list = reader.get_all_topics_and_types()

      # Create a map for quicker lookup
      topic_dict = {topic_list[i].name: topic_list[i] for i in range(len(topic_list))}

      print('reading Topic Records!')
      if reader.has_next():
        (topic, data, ts) = reader.read_next()
        if start == 0:
          start = ts
        write_topic(wr_f, topic_dict[topic].type, topic, data, ts)

      while reader.has_next():
        (topic, data, ts) = reader.read_next()
        write_topic(wr_f, topic_dict[topic].type, topic, data, ts)

  start_date = '{}.{}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(start / 1000 / 1000 / 1000)), start % (1000 * 1000 * 1000))
  end_date = '{}.{}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(ts / 1000 / 1000 / 1000)), ts % (1000 * 1000 * 1000))
  delta = str(datetime.timedelta(seconds=(ts - start) / 1000 / 1000 / 1000))

  # Generate result
  with open(path, mode='w') as w:
    w.write('start,{},end,{},duration,{:.3f},sec'.format(start_date, end_date, (ts - start) / 1000 / 1000 / 1000))
    w.write(',\"Caution:Timestamp timezone is converting timezone not recording timezone!\"\n')
    for file in files:
      w.write('{}\n'.format(file))
    w.write('interval,timestamp,topic,level,message\n')

    with open("tmp.csv", mode='r') as r:
      w.write(r.read())

    os.remove("tmp.csv")
    print('created {}'.format(path))

  # Generate intervals
  with open(path_interval, mode='w') as w:
    w.write('duration,{}\n'.format(delta))
    w.write('topic,average,min,max\n')
    for key, value in intervals.items():
      w.write('{},{:.03f},{:.03f},{:.03f}\n'.format(key, sum(value)/len(value), min(value), max(value)))


def main():
  global filter

  if (argc < 2):
    print('Usage: # python3 %s source (filter)' % argvs[0])
    quit()

  if (argc >= 3): 
    filter = argvs[2]

  process(argvs[1])


if __name__ == '__main__':
  main()
