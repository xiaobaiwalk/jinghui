# -*- coding: utf-8 -*-
import requests, shutil, sys, os, re, time
import wget as Wget
import logging

logger = logging.getLogger("logd")


class wget:
    def __init__(self, config={}):
        self.config = {
            'block': int(config['block'] if config.has_key('block') else 1024),
        }
        self.total = 0
        self.size = 0
        self.filename = ''

    def touch(self, filename):
        outnewpath = os.path.dirname(filename)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        with open(filename, 'w') as fin:
            pass

    #
    # def remove_nonchars(self, name):
    #     (name, _) = re.subn(ur'[\\\/\:\*\?\"\<\>\|]', '', name)
    #     return name

    def support_continue(self, url):
        headers = {
            'Range': 'bytes=0-4'
        }
        try:
            r = requests.head(url, headers=headers)
            crange = r.headers['content-range']
            self.total = int(re.match(ur'^bytes 0-4/(\d+)$', crange).group(1))
            return True
        except:
            pass
        return False

    def download(self, url, filename, headers={}):
        if url.startswith("ftp"):
            return self.download_wget(url, filename)
        else:
            return self.download_http(url, filename)

    def download_wget(self, url, filename, headers={}):
        try:
            outnewpath = os.path.dirname(filename)
            if not os.path.exists(outnewpath):
                os.makedirs(outnewpath)
            Wget.download(url, filename)
            return True
        except:
            logger.debug("download_wget Error: %s" % str(sys.exc_info()))
            return False

    def download_http(self, url, filename, headers={}):
        finished = False
        block = self.config['block']
        local_filename = filename
        tmp_filename = local_filename + '.tmp'
        size = self.size

        if self.support_continue(url):  # 支持断点续传
            try:
                self.size = os.stat(tmp_filename).st_size
                size = self.size
            except Exception as e:
                logger.error("get {} size error".format(tmp_filename),
                             exec_info=True)
                self.touch(tmp_filename)
            finally:
                headers['Range'] = "bytes=%d-" % (self.size,)
        else:
            self.touch(tmp_filename)

        s = requests.session()
        s.keep_alive = False
        r = requests.get(url, stream=True, verify=False, headers=headers)

        start_t = time.time()
        with open(tmp_filename, 'ab+') as f:
            f.seek(self.size)
            f.truncate()
            try:
                for chunk in r.iter_content(chunk_size=block):
                    if chunk:
                        f.write(chunk)
                        size += len(chunk)
                        f.flush()
                finished = True
                spend = int(time.time() - start_t)
                speed = int((size - self.size) / 1024 / spend if spend > 0 else 1)
                logger.info(
                    'Download %s Finished!Total Time: %ss, Download Speed: %sk/s' % (local_filename, spend, speed))
            except:
                logger.error("Download %s pause. %s" % (local_filename, str(sys.exc_info())))
                return False
            finally:
                if finished:
                    self.moveFile(tmp_filename, local_filename)
                    return True

    def moveFile(self, src_file, dest_file):
        d_path = "/".join(dest_file.split("/")[:-1])
        if not os.path.isdir(d_path):
            os.makedirs(d_path)
        if os.path.exists(dest_file):
            os.remove(dest_file)
        shutil.move(src_file, dest_file)
        print src_file, dest_file
        logger.info("moveFile %s to %s" % (src_file, dest_file))
