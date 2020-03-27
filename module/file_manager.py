# -*- coding: utf-8 -*-

"""Case Semantix

Cria a classe Downloader, que nesse momento tem apenas
a função de realizar download dos arquivos de entrada
"""

import shutil
import logging
import os
import re
from urllib import request, error
from contextlib import closing


class FileManager:
    """Gerencia as funções e atributos de arquivos"""
    def __init__(self, file, dir):
        self.file = file
        self.dir = dir

    def download_file(self):
        """Faz o download de arquivos, atraves do pacote request"""
        filename = self.file.split('/')[-1]
        file_path = os.path.abspath(self.dir + filename)
        if not os.path.exists(self.dir):
            os.mkdir(os.path.abspath(self.dir))
        if os.path.exists(file_path):
            logging.info('Arquivo já existente "{}"'.format(self.file))
        else:
            try:
                logging.info('Iniciando download do arquivo {}'.format(self.file))
                with closing(request.urlopen(self.file)) as r:
                    with open(file_path, 'wb') as f:
                        shutil.copyfileobj(r, f)
                logging.info('Download finalizado do arquivo "{}"'.format(file_path))
                self.set_file(filename)
            except error.URLError as url:
                logging.warning('Não foi possível fazer o download do arquivo {}'.format(self.file))
                logging.warning(url.reason)
        return

    def set_file(self, file):
        """Altera o valor do atributo file"""
        self.file = file

    def parse_log_file(self, row, pattern):
        """Procura por padrão em cada linha"""
        match = re.match(pattern, row)
        if match is None:
            return (row, 'not match')
        return (match.groups(), 'match')