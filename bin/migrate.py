#!/usr/bin/env python

import argparse
import os
import struct
import mmap
import socket

try:
    import whisper
except ImportError:
    raise SystemExit('[ERROR] Please make sure whisper is installed properly')

__author__ = 'gregory'


def mmap_file(filename):
    fd = os.open(filename, os.O_RDONLY)
    map = mmap.mmap(fd, os.fstat(fd).st_size, prot=mmap.PROT_READ)
    os.close(fd)
    return map

def read_archives(map, count):
    try:
        (aggregationType, maxRetention, xFilesFactor, archiveCount) = struct.unpack(whisper.metadataFormat,
                                                                                    map[:whisper.metadataSize])
    except:
        raise whisper.CorruptWhisperFile("Unable to unpack header")

    archiveOffset = whisper.metadataSize
    if count > archiveCount:
        count = archiveCount

    archSamples = []
    for i in xrange(count):
        try:
            (offset, secondsPerPoint, points) = struct.unpack(whisper.archiveInfoFormat, map[
                                                                                         archiveOffset:archiveOffset + whisper.archiveInfoSize])
        except:
            raise whisper.CorruptWhisperFile("Unable to read archive %d metadata" % i)
        samples = []
        for point in xrange(points):
            (timestamp, value) = struct.unpack(whisper.pointFormat, map[offset:offset + whisper.pointSize])
            if timestamp != 0:
                samples.append((timestamp, value))
            offset += whisper.pointSize

        archiveOffset += whisper.archiveInfoSize
        archSamples.append(samples)

    return archSamples

def send_data_samples_to_atsd(socket, metric, points):
    for (timestamp, val) in points:
        socket.sendall("%s %s %s\n" % (metric, val, timestamp))

def export_file(path, whisperRoot, socket):
    map = mmap_file(path)
    samples, = read_archives(map, 1)
    (path_without_extension, _) = os.path.splitext(path)
    (_, metric) = path_without_extension.split(whisperRoot+"/")
    metric = metric.replace("/", ".")
    send_data_samples_to_atsd(socket, metric, samples)

parser = argparse.ArgumentParser(description='Migrate whisper data to Axibase Time Series Database.')

parser.add_argument('path', help='path to folder/file that will be exported to ATSD. Path must be specified either: directly to .wsp files (only if -R is not set) OR to folders containing the .wsp files (-R must be set). ~ symbol cannot be used.')
parser.add_argument('hostname', help='ATSD hostname')
parser.add_argument('port', help='ATSD listening port', type=int)
parser.add_argument('--whisper-base', help='base path to which all metric names will be resolved (default: ".")', dest="whisperRoot", default=os.path.curdir, metavar="BASE")
parser.add_argument('-R', action='store_true', help='export recursively all files in specified folder', dest="isRecursive")

args = parser.parse_args()

PATH = os.path.abspath(args.path)
IS_RECURSIVE = args.isRecursive
ADDRESS = (args.hostname, args.port)
WHISPER_ROOT = os.path.abspath(args.whisperRoot)

if not os.path.exists(PATH):
    raise SystemExit('[ERROR] File "%s" does not exist!' % PATH)
if not os.path.exists(WHISPER_ROOT):
    raise SystemExit('[ERROR] Whisper root "%s" does not exist!' % WHISPER_ROOT)
if not PATH.startswith(WHISPER_ROOT):
    raise SystemExit('[ERROR] Wrong whisper root "%s" should be prefix of path "%s"!' % (WHISPER_ROOT, PATH))


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(ADDRESS)

if IS_RECURSIVE and os.path.isdir(PATH):
    for root, _, files in os.walk(PATH):
        for filename in files:
            if os.path.splitext(filename)[1] == ".wsp":
                export_file(os.path.join(root, filename), WHISPER_ROOT, s)
else:
    if not os.path.isfile(PATH):
        raise SystemExit('[ERROR] "%s" is not a file! Maybe you need to specify -R?' % os.path.abspath(PATH))
    export_file(PATH, WHISPER_ROOT, s)

s.close()