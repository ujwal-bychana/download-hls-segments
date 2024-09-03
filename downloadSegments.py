#!/usr/bin/env python3
# Http Live Streaming -- fetcher/downloader
# A simple script to download ts segments/m3u8 files from given url, including variant streams from master m3u8
# require: python3

import sys
import os
import urllib.request
import aiohttp
import aiofiles
import asyncio

# source to download the manifest from
TEST_URL = 'https://demo.unified-streaming.com/k8s/features/stable/video/tears-of-steel/tears-of-steel.ism/master.m3u8'


def isValidUrl(url: str) -> bool:
    if url == '':
        print('Invalid URL: empty url')
        return False
    elif not (url.startswith('http') or url.startswith('https')):
        print('Invalid URL: require \'http/https\' url')
        return False
    elif os.path.splitext(url)[1].lower() != '.m3u8':
        print('Invalid URL: not hls source')
        return False
    else:
        return True

def readDataFromUrl(url: str) -> bytes:
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
        return data
    except:
        pass

def writeFile(path: str, filename: str, data: bytes) -> None:
    fullPath = os.path.join(path, filename)
    with open(fullPath, 'wb') as file:
        file.write(data)
    return None

failedUrl = []
async def readDataFromUrlTs(session : aiohttp.ClientSession, object) -> bytes:
    print(object["url"])
    try:
        async with session.get(url=object["url"], timeout=3000) as response:
            data = await response.read()
            await writeFileTs(object["baseDir"], object["line"], data)
    except Exception as e:
        failedUrl.append(object)


async def writeFileTs(path: str, filename: str, data: bytes) -> None:
    try:
        fullPath = os.path.join(path, filename)
        dir = "/".join(fullPath.split("/")[:-1])#.join("/")
        if not os.path.exists(dir):
            try:
                os.mkdir(dir)
            except Exception as e:
                print(e, ' Create ', dir, ' failed, exit.')
        async with aiofiles.open(fullPath, 'wb') as file:
            await file.write(data)
    except Exception as e:
        print(e)
    return None

downloadUrls = []
async def download() -> None:
    chunks = [downloadUrls[x:x+50] for x in range(0, len(downloadUrls), 50)]
    async with aiohttp.ClientSession() as session:
        # for object in downloadUrls:
        for chunk in chunks:
            await asyncio.gather(*(readDataFromUrlTs(session, object) for object in chunk))
            # tsData = readDataFromUrl(object[url], session)
            # writeFile(object.baseDir, object.line, tsData)
def parseM3U8(baseDir: str, baseUrl: str, data: bytes) -> None:
    global downloadUrls
    for line in data.splitlines():
        line = line.strip()
        extension = os.path.splitext(line)[1]
        if 0 or extension.lower() == b'.ts' or extension.lower() == b'.aac' or extension.lower() == b'.webvtt'  or extension.lower() == b'.jpg' or extension.lower() == b'.m4s' or extension.lower() == b'.mp4':
            tsUrl = baseUrl + '/' + line.decode()
            os.path.join(baseDir, line.decode())
            if not os.path.exists(os.path.join(baseDir, line.decode())):
                downloadObject = {}
                if (not os.path.exists(os.path.join(baseDir, line.decode()))):
                    downloadObject["url"] = tsUrl
                    downloadObject["baseDir"] = baseDir
                    downloadObject["line"] = line.decode()
                    downloadUrls.append(downloadObject)
        elif extension.lower() == b'.m3u8':
            simpleUrl = baseUrl + '/' + line.decode()
            if "##" not in simpleUrl:
                binDir = os.path.join(baseDir, simpleUrl.split('/')[-2])
                m3u8Name = os.path.basename(simpleUrl)
                print('In master m3u8, processing ', simpleUrl)
                if not os.path.exists(binDir):
                    try:
                        os.mkdir(binDir)
                    except Exception as e:
                        print(e, ' Create ', binDir, ' failed, exit.')
                        return
                m3u8Data = readDataFromUrl(simpleUrl)
                writeFile(binDir, m3u8Name, m3u8Data)
                parseM3U8(binDir, os.path.dirname(simpleUrl), m3u8Data)
        elif not line.startswith(b'#EXT-X-I-FRAME-STREAM-INF') and extension.lower() == b'.m3u8"':
            simpleUrl = baseUrl + '/' + line.decode().split('URI="')[1].replace('"', '')
            binDir = os.path.join(baseDir, simpleUrl.split('/')[-2])
            m3u8Name = os.path.basename(simpleUrl)
            print('In master m3u8, processing ', simpleUrl)
            if not os.path.exists(binDir):
                try:
                    os.mkdir(binDir)
                except Exception as e:
                    print(e, ' Create ', binDir, ' failed, exit.')
                    return
            m3u8Data = readDataFromUrl(simpleUrl)
            writeFile(binDir, m3u8Name, m3u8Data)
            parseM3U8(binDir, os.path.dirname(simpleUrl), m3u8Data)
        elif line.startswith(b'#EXT-X-I-FRAME-STREAM-INF'):
            simpleUrl = baseUrl + '/' + line.decode().split('URI="')[1].replace('"', '')
            binDir = os.path.join(baseDir, simpleUrl.split('/')[-2])
            m3u8Name = os.path.basename(simpleUrl)
            print('In master m3u8, processing ', simpleUrl)
            if not os.path.exists(binDir):
                try:
                    os.mkdir(binDir)
                except Exception as e:
                    print(e, ' Create ', binDir, ' failed, exit.')
                    return
            m3u8Data = readDataFromUrl(simpleUrl)
            writeFile(binDir, m3u8Name, m3u8Data)
            parseM3U8(binDir, os.path.dirname(simpleUrl), m3u8Data)
        elif line.startswith(b'#EXT-X-MAP'):
            line = line.strip()
            mapURI = line.decode().split('URI="')[1].split('"')[0]
            downloadObject = {}
            if (not os.path.exists(os.path.join(baseDir, mapURI))):
                downloadObject["url"] = baseUrl + '/' + mapURI
                downloadObject["baseDir"] = baseDir
                downloadObject["line"] = mapURI
                downloadUrls.append(downloadObject)


def fetchData(url: str) -> bool:
    curPath = os.path.abspath(os.curdir)
    baseUrl = os.path.dirname(url)
    m3u8Name = os.path.basename(url)
    binDir = os.path.join(curPath, url.split('/')[-2])
    print(curPath, baseUrl, binDir)
    if not os.path.exists(binDir):
        try:
            os.mkdir(binDir)
        except Exception as e:
            print(e, ' Create ', binDir, ' failed, exit.')
            return False
    m3u8Data = readDataFromUrl(url)
    writeFile(binDir, m3u8Name, m3u8Data)
    parseM3U8(binDir, baseUrl, m3u8Data)
    return True


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print('Invalid arguments: require 1 parameter, but you gave ', len(sys.argv) - 1)
    #     exit(-1)
    # url = sys.argv[1]
    url = TEST_URL
    # if isValidUrl(url):
    fetchData(url)
    asyncio.run(download())
    print('Done')
    # else:
    #     exit(-1)