# runner script to run the Word Count

from project1 import MRWordFrequencyCount
import json

if __name__ == '__main__':
    
    myjob1 = MRWordFrequencyCount()
    with myjob1.make_runner() as runner:
        runner.run()
