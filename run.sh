#!/bin/bash
python3 project1.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.4.jar -r  $1 > output2.txt