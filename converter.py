#!/usr/bin/python

# @author Tushar Kant <tushar91delete@gmail.com>

# This script will generate the video in different formats

# 720p
# Resolution:  1280x720 = 2000
# Video Bitrate Range: 1,500 - 4,000 Kbps @ 2500 kbps
# 480p
# Resolution:  854x480 = 1334
# Video Bitrate Range: 500 - 2,000 Kbps @ 1600 kbps
# 240p
# Resolution:  426x240 = 666
# Video Bitrate Range: 300 - 700 Kbps @ 500 kbps

# @todo 360p
# Resolution:  640x360 = 1000
# Video Bitrate Range: 400 - 1,000 Kbps @ 800 kbps


import subprocess
import sys
import getopt
import json
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor

WORKER_COUNT = 10
LOG_FILE = '/var/log/py_hls_generator.log'


def get_info(file):
    """Gets information about a video using ffprobe

    Args:
        file: Absolute path of the input file
    """
    cmd = "ffprobe -v error -select_streams v:0  -print_format json -show_streams -i '{}'"
    p = subprocess.Popen(
        cmd.format(file),
        stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    p_status = p.wait()

    logging.info('Ran command {}, exit code was: {}'.format(cmd.format(file), p_status))

    output = output.decode()
    output = json.loads(output)

    return output['streams'][0]


def converter(resolution, file, output_dir, filename):
    """Converts video into the resolution provided

    Args:
        resolution: Generate video of provided resolution
        file: Absolute path for the input video
        output_dir: Directory where all the stream will be placed
        filename: Output filename, the dir and ts/m3ud files will be created with this name
    """
    logging.info('Converting uploaded video in {}'.format(resolution))

    cmd = ''
    if str(resolution) == '220':
        if not os.path.exists('{}/{}-500k'.format(output_dir, filename)):
            os.makedirs('{}/{}-500k'.format(output_dir, filename), exist_ok=True)

        cmd = "ffmpeg -i '{}' -acodec aac -strict -2 -ab 64k -map 0  -f segment -vbsf h264_mp4toannexb -flags  \
              -global_header  -segment_format mpegts -segment_list_type m3u8 -vcodec libx264  \
              -vf \"yadif=0, scale=640:360\" -crf 20 -b:v 450k -maxrate 450k -bufsize 450k -g 30  \
              -segment_list '{}/{}-500k/{}-500k.m3u8'  '{}/{}-500k/{}-%04d.500k.ts'"
    elif str(resolution) == '480':
        if not os.path.exists('{}/{}-1M'.format(output_dir, filename)):
            os.makedirs('{}/{}-1M'.format(output_dir, filename), exist_ok=True)

        cmd = "ffmpeg -i '{}' -acodec aac -strict -2 -ab 128k -map 0  -f segment -vbsf h264_mp4toannexb -flags \
              -global_header  -segment_format mpegts -segment_list_type m3u8 -vcodec libx264 \
              -vf \"yadif=0, scale=1280:720\" -crf 20 -b:v 900k -maxrate 900k -bufsize 900k -g 30 \
              -segment_list '{}/{}-1M/{}-1M.m3u8'  '{}/{}-1M/{}-%04d.1M.ts'"
    elif str(resolution) == '720':
        if not os.path.exists('{}/{}-3M'.format(output_dir, filename)):
            os.makedirs('{}/{}-3M'.format(output_dir, filename), exist_ok=True)

        cmd = "ffmpeg -i '{}' -acodec copy -map 0  -f segment -vbsf h264_mp4toannexb -flags \
              -global_header -segment_format mpegts -segment_list_type m3u8 -vcodec libx264 -crf 20 -b:v 2600k \
              -maxrate 2600k -bufsize 2600k -g 30 -segment_list '{}/{}-3M/{}-3M.m3u8'  '{}/{}-3M/{}-%04d.3M.ts'"

    cmd = cmd.format(file, output_dir, filename, filename, output_dir, filename, filename)

    logging.info('Running Command: {}'.format(cmd))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    p_status = p.wait()
    logging.info('Ran command {}, exit code was: {}'.format(cmd.format(file), p_status))
    if err:
        logging.error('Unable to execute command: {}, got error: {}'.format(cmd, err))
        sys.exit(255)

    return


def manifest_generator(output_dir, filename, streams):
    """Generates the main manifest file based on the inputs, to adaptively select the best available stream

    Args:
        output_dir: Directory where all the stream are present
        filename:  Output filename, the dir and ts/m3ud files will be created with this name
        streams: List of generated streams for the give file
    """
    logging.info("Generating main manifest file at: {}/{}.m3u8, for streams: {}".
                 format(output_dir, filename, ', '.join(str(x) for x in streams)))

    target = open('{}/{}.m3u8'.format(output_dir, filename), 'a')
    target.truncate()
    target.write('#EXTM3U' + "\n")
    target.write('#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=500000' + "\n")
    target.write('{}-500k/{}-500k.m3u8{}'.format(filename, filename, "\n"))

    if 480 in streams:
        target.write('#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=1000000' + "\n")
        target.write('{}-1M/{}-1M.m3u8{}'.format(filename, filename, "\n"))

    if 720 in streams:
        target.write('#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=3000000' + "\n")
        target.write('{}-3M/{}-3M.m3u8{}'.format(filename, filename, "\n"))

    target.close()

    logging.info("Generated main manifest file at: {}/{}.m3u8, for streams: {}".
                 format(output_dir, filename, ', '.join(str(x) for x in streams)))

    return


def decider(stream_info, i_file, output_dir, filename):
    """Decides how many resolutions have to be generated for the provided video

    Args:
        stream_info: List with detailed info about video stream
        i_file: Absolute path of the input file
        output_dir: Directory where all the stream will be placed
        filename: Output filename, the dir and ts/m3ud files will be created with this name
    """
    bit_rate = int(stream_info['bit_rate'])
    total_resolution = int(stream_info['width'] + stream_info['height'])
    start_time = time.time()
    logging.info("Width: {}, Height: {}, Bit-Rate: {} kbps and Total-Resolution: {}.".format(stream_info['width'],
                                                                                             stream_info['height'],
                                                                                             str(bit_rate / 1000),
                                                                                             total_resolution))

    resolutions = []
    if total_resolution >= 2000 and bit_rate >= 1500:
        resolutions.extend([720, 480, 220])

    elif 1334 <= total_resolution < 2000 and 500 <= bit_rate < 2000:
        resolutions.extend([480, 220])

    else:
        resolutions.append(220)

    executor = ThreadPoolExecutor(max_workers=WORKER_COUNT)

    for resolution in resolutions:
        executor.submit(converter, resolution, i_file, output_dir, filename)

    executor.shutdown(wait=True)

    manifest_generator(output_dir, filename, resolutions)

    end_time = time.time()

    logging.info('Total time taken by the complete conversion process is {}'.format(int(end_time - start_time)))

    return


def main(argv):
    input_file = ''
    output_dir = ''
    filename = ''

    try:
        opts, args = getopt.getopt(argv, "hi:o:f:")
    except getopt.GetoptError:
        print('converter.py -i <input_file> -o <output_directory> -f <name_of_files>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('converter.py -i <input_file> -o <output_directory> -f <name_of_files>')
            sys.exit()

        ###############################
        # o == option
        # a == argument passed to the o
        ###############################
        for o, a in opts:
            if o == "-i":
                input_file = a
            elif o == "-o":
                output_dir = a
            elif o == "-f":
                filename = a
            else:
                assert False, "unhandled option"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')
    logging.info('Input-file: {}, Output-directory: {}, filename: {}'.format(input_file, output_dir, filename))
    decider(get_info(input_file), input_file, output_dir, filename)

if __name__ == "__main__":
    main(sys.argv[1:])
