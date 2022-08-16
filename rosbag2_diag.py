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


def get_rosbag_options(path, serialization_format='cdr'):
  storage_options = rosbag2_py.StorageOptions(uri=path, storage_id='sqlite3')

  converter_options = rosbag2_py.ConverterOptions(
      input_serialization_format=serialization_format,
      output_serialization_format=serialization_format)

  return storage_options, converter_options


interval = {}


def write_topic(f, type_map, topic, data, ts):
  global interval

  if topic != '/diagnostics' and topic != '/system/emergency/hazard_status':
    return

  # Create timestamp
  date = '\'{}.{}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(ts / 1000 / 1000 / 1000)), ts % (1000 * 1000 * 1000))

  # Deserialize topic
  msg_type = get_message(type_map[topic])
  try:
    msg = deserialize_message(data, msg_type)
  except Exception as e:
    print(e)
    return

  if topic == '/diagnostics':
    for status in msg.status:
      level = int.from_bytes(status.level, 'big')
      key_value = ""
      # Append diagnotic array
      for value in status.values:
        key_value += '{},{},'.format(value.key, value.value)

      # If the topic already read
      if status.name in interval:
        # Write interval
        f.write('{:.3f},'.format((ts - interval[status.name]) / 1000 / 1000 / 1000))
        f.write('{},{},{},{},{}\n'.format(date, status.name, level, status.message, key_value))
      else:
        f.write(',{},{},{},{},{}\n'.format(date, status.name, level, status.message, key_value))
        interval[status.name] = 0

      interval[status.name] = ts

  elif topic == '/system/emergency/hazard_status':
    lf_value = ""
    for lf in msg.status.diagnostics_lf:
      lf_value += '{}|{}|'.format(lf.level, lf.name, lf.message)
    spf_value = ""
    for spf in msg.status.diagnostics_spf:
      spf_value += '{}|{}|'.format(spf.level, spf.name, spf.message)

    f.write(',{},{},{},LF={},SPF={}\n'.format(date, topic, msg.status.level, lf_value, spf_value))

  else:
    f.write('\n')


def process(source):

  path = datetime.datetime.now().strftime('rosbag_%Y%m%d-%H%M%S.csv')

  # Generate temporary file
  with open("tmp.csv", mode='w') as f:

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

      topic_types = reader.get_all_topics_and_types()

      # Create a map for quicker lookup
      type_map = {topic_types[i].name: topic_types[i].type for i in range(len(topic_types))}

      if reader.has_next():
        (topic, data, ts) = reader.read_next()
        if start == 0:
          start = ts
        write_topic(f, type_map, topic, data, ts)

      while reader.has_next():
        (topic, data, ts) = reader.read_next()
        write_topic(f, type_map, topic, data, ts)

  start_date = '{}.{}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(start / 1000 / 1000 / 1000)), start % (1000 * 1000 * 1000))
  end_date = '{}.{}'.format(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(ts / 1000 / 1000 / 1000)), ts % (1000 * 1000 * 1000))

  # Generate result
  with open(path, mode='w') as w:
    w.write('start,{},end,{},duration,{:.3f},sec\n'.format(start_date, end_date, (ts - start) / 1000 / 1000 / 1000))
    for file in files:
      w.write('{}\n'.format(file))
    w.write('interval,timestamp,topic,level,message\n')

    with open("tmp.csv", mode='r') as r:
      w.write(r.read())

    os.remove("tmp.csv")


def main():
  if (argc != 2):
    print('Usage: # python3 %s source' % argvs[0])
    quit()

  process(argvs[1])


if __name__ == '__main__':
  main()
