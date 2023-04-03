import subprocess
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
import os

# Function to run pubsubtest3 which will find average front and back sight of cars
class RunSubprocessFn3(beam.DoFn):
    def process(self, element):
        cmd = ['python', 'pubsubtest3.py']
        result = subprocess.run(cmd, capture_output=True)
        print(result.stdout.decode())

# Function to run pubsubtest4 which will find number of each type of car
class RunSubprocessFn4(beam.DoFn):
    def process(self, element):
        cmd = ['python', 'pubsubtest4.py']
        result = subprocess.run(cmd, capture_output=True)
        print(result.stdout.decode())

# Function to run pubsubtest5 which will find average max speed of cars and find the amount going faster than it
class RunSubprocessFn5(beam.DoFn):
    def process(self, element):
        cmd = ['python', 'pubsubtest5.py']
        result = subprocess.run(cmd, capture_output=True)
        print(result.stdout.decode())

# Function to run pubsubtest6 which will find average speed in each lane
class RunSubprocessFn6(beam.DoFn):
    def process(self, element):
        cmd = ['python', 'pubsubtest6.py']
        result = subprocess.run(cmd, capture_output=True)
        print(result.stdout.decode())

# Function to run pubsubtest7 which will find number of cars that have enter a lane and the number of lane changes made
class RunSubprocessFn7(beam.DoFn):
    def process(self, element):
        cmd = ['python', 'pubsubtest7.py']
        result = subprocess.run(cmd, capture_output=True)
        print(result.stdout.decode())

def run(argv=None):
    # Defining pipeline options to make it more user friendly
    pipeline_options = PipelineOptions([
        '--region northamerica-northeast2',
        'runner DataflowRunner',
        '--project nodal-time-375522',
        '--temp_location gs://nodal-time-375522-bucket/tmp',
        '--staging_location gs://nodal-time-375522-bucket/staging',
        '--setup_file ./setup.py ' 
    ])
    with beam.Pipeline(options=pipeline_options) as p:
        dataflow = p | "Create dataflow" >> beam.Create([1])
        output1 = dataflow | "Run task 3" >> beam.ParDo(RunSubprocessFn3())
        output2 = dataflow | "Run task 4" >> beam.ParDo(RunSubprocessFn4())
        output3 = dataflow | "Run task 5" >> beam.ParDo(RunSubprocessFn5())
        output4 = dataflow | "Run task 6" >> beam.ParDo(RunSubprocessFn6())
        output5 = dataflow | "Run task 7" >> beam.ParDo(RunSubprocessFn7())

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()