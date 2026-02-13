#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  2 13:13:58 2023

@author: hagen
"""
import pathlib as _pl
import configparser as _cp
# import numpy as _np
import traceback as _tb
from email.mime.text import MIMEText as _MIMEText
import smtplib as _smtplib
import pandas as _pd
import socket


class Reporter(object):
    def __init__(self, 
                 name = None, 
                 # log_folder = '/export/htelg/tmp/'
                 log_folder = '/home/grad/htelg/.processlogs/',
                 verbose = True, 
                 reporting_frequency = (1,'h'),
                 ):
        self.path2log = _pl.Path(log_folder).joinpath(f'{name}.log')
        self.reset()
        self.name = name
        self.reporting_frequency = _pd.to_timedelta(*reporting_frequency)
        self.verbose = verbose
        self.starttime = _pd.Timestamp.now()
        if verbose:
            print(f'start time: {self.starttime}')
    
    def reset(self):
        self._clean = 0
        self._warning = 0
        self._error = 0
        self._starttime = _pd.Timestamp.now()
    
    @property
    def clean(self):
        return self._clean
    
    def clean_increment(self, value = 1):
        self._clean += value
    
    @property
    def warnings(self):
        return self._warning
    
    def warnings_increment(self, value = 1):
        self._warning += value
    
    @property
    def errors(self):
        return self._error
    
    def errors_increment(self, value = 1):
        self._error += value
    
    @property
    def settings(self):
        if isinstance(self._settings, type(None)):
            if not self.path2config.is_file():
                self.generate_default_config()
            # else:
            self._load_config()
        return self._settings
    
    def _load_config(self):
        config = _cp.ConfigParser()
        config.read(self.path2config)
        self._settings = config
        return config
    
    @property
    def report_due(self):
        return (_pd.Timestamp.now() - self._starttime) > self.reporting_frequency
    
    @property
    def next_report_due(self):
        return self._starttime + self.reporting_frequency
    
    def log(self, reset_counters = True, overwrite_reporting_frequency = False):
        if self.report_due or overwrite_reporting_frequency:
            #create file if it does not exist
            file_exists =  self.path2log.is_file()
            
            if not file_exists:
                with open(self.path2log, 'w') as log_out:
                    log_out.write('datetime,rund_status,error,success,warning,subprocess,server,comment\n')
            with open(self.path2log, 'a') as log_out:
                log_out.write(self.create_log_data())
            if reset_counters:
                self.reset()
            return True
        else:
            return False
    
    def generate_default_config(self):
        settings = ('[notify]\n'
                    'email_address = None\n'
                    'smtp = localhost')
        # if not self.path2config.parent.is_dir():
        self.path2config.parent.mkdir(exist_ok=True)
        with open(self.path2config, 'w') as raus:
            raus.write(settings)
        # self._settings = settings
        
    def create_log_data(self):
        datetime = _pd.Timestamp.now()
        run_status = 0 #### TODO: this is not really used right now
        # success = self.the_automated_process.no_processed_success
        # error = self.the_automated_process.no_processed_error
        # warning = self.the_automated_process.no_processed_warning
        subprocess = self.name#### TODO: not sure what this was ment for in the early days
        server = socket.gethostname()
        comment = ''
        log_string = f'{datetime},{run_status},{self.errors},{self.clean},{self.warnings},{subprocess},{server},{comment}\n'
        return log_string
    
    def parse_email(self, test = True):
        """
        Probably not working right now ... I kind of went away from reporting via email.

        Parameters
        ----------
        test : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        msg : TYPE
            DESCRIPTION.

        """
        config = self.settings
        assert(config.get('notify', 'email_address') != 'None'), f'No email has been specified, _please do so in ~/.{self.product_name}/config.ini'
        
        # out = self._last_processing
        # messages = ['run started {}'.format(out['start_time'])]
        messages = ['run started {}'.format(self.start_time)]
        messages.append('run finshed {}'.format(_pd.Timestamp.now()))
        
        #### summary
        # no_of_errors = len(out['errors'])
        # no_of_files_processed = out['no_of_files_processed']
        fail = False

        try:
            if test:
                assert(False), 'sabotage!!!'
            no_of_files_that_need_processing = self.the_automated_process.no_of_files_that_need_processing
            no_of_files_processed = self.the_automated_process.no_of_files_processed
            no_of_errors = self.the_automated_process.no_of_errors
            subject = '{product_name} - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})' 
            msgt = '\n'.join([f'length of workplan:\t\t{no_of_files_that_need_processing}',f'successfully created files:\t{no_of_files_processed}', f'errors encountered:\t\t{no_of_errors}'])

            messages.append(msgt)
                    #### errors
            # if no_of_errors != 0:
            #     errs = out['errors']
            #     err_types = _np.array([err.__class__.__name__ for err in errs])
                
            #     typoferrors = []
            #     for toe in _np.unique(err_types):
            #         typoferrors.append({'name': toe, 'times': (err_types == toe).sum(), 'first_err': errs[_np.where(err_types == toe)[0][0]]})
            
            #     messages.append('\n'.join(['Errors by type:',] + ['\t{tn}:\t{tt}'.format(tn = toe['name'], tt = toe['times']) for toe in typoferrors]))
            #     messages.append('\n=============================================\n'.join(['First traceback for each error type',]+[''.join(_tb.format_tb(toe['first_err'].__traceback__) + [toe['first_err'].__str__(),]) for toe in typoferrors]))
            

            
            #### subject
            if no_of_errors ==0:
                status = 'clean'
            else:
                status = 'errors'
            # subject = f'cl51cloudprod - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})'
            
            subject = subject.format(product_name = self.product_name, status = status, no_of_files_processed = no_of_files_processed, no_of_errors = no_of_errors)
        
        except:                
            fail = _tb.format_exc()
            subject = f'{self.product_name} - status: Fail'
            messages.append(fail)
        
        #### email body
        message_txt = '\n=========================================================================\n'.join(messages)
        msg = _MIMEText(message_txt)
        address  = config.get('notify', 'email_address')
        # smtp = config.get('notify', 'smtp')
        
        msg['Subject'] = subject
        msg['From'] = address
        msg['To'] = address
        return msg
    
    def notify(self,
               # product_name,
               # no_of_files_processed,
               # start_time,
               # finish_time,
               # no_of_errors = None
               # subject = 'cl51cloudprod - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})',
               ):

        
        # Send the message via our own SMTP server.
        smtp = self.settings.get('notify', 'smtp')
        s = _smtplib.SMTP(smtp)
        s.send_message(self.parse_email())
        s.quit()
        
    def wrapup(self):
        """
        Sends out the last log and prints some info. Note processing results
        (clean, warning, errors) might not be the total number of process, but 
        the numbers since the last reset, which usually happens during a log.

        Returns
        -------
        None.

        """
        if self.verbose:
            print(f'number of cleanes: {self.clean}')
            print(f'number of errors: {self.errors}')
            print(f'number of warnings: {self.warnings}')
            endtime = _pd.Timestamp.now()
            print(f'time finished: {endtime}')
            duration = (endtime - self.starttime) / _pd.to_timedelta(1, 'h')
            print(f'total processing time: {duration} hours')
        self.log(reset_counters = False, overwrite_reporting_frequency = True)
    

class Automation(object):
    def __init__(self, the_automated_process, product_name = None):
        """ This strategy of reporting has turned out to be disadvantage over the Reporter class, in particular for parallelized processes
        The automation instance helps you with notification and logging of the automated process.
        Typically the process would be run in a crone job.
        
        The automated process has to be an object that has (after execution) the following attributes:
            no_processed_success
            no_processed_error
            no_processed_warning

        Parameters
        ----------
        the_automated_process : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Example
        --------

        """
        self.the_automated_process = the_automated_process
        if isinstance(product_name, type(None)):
            self.product_name = the_automated_process.__class__.__name__
        else:
            self.product_name = product_name
            
        self.path2config = _pl.Path.home().joinpath(f'.{self.product_name}/config.ini')
        
        self.path2log = _pl.Path(f'/home/grad/htelg/.processlogs/{self.product_name}.log')
        self._settings = None
        self.start_time = _pd.Timestamp.now()
    
    def generate_default_config(self):
        settings = ('[notify]\n'
                    'email_address = None\n'
                    'smtp = localhost')
        # if not self.path2config.parent.is_dir():
        self.path2config.parent.mkdir(exist_ok=True)
        with open(self.path2config, 'w') as raus:
            raus.write(settings)
        # self._settings = settings
    
    def _load_config(self):
        config = _cp.ConfigParser()
        config.read(self.path2config)
        self._settings = config
        return config
    
    @property
    def settings(self):
        if isinstance(self._settings, type(None)):
            if not self.path2config.is_file():
                self.generate_default_config()
            # else:
            self._load_config()
        return self._settings
     
    def create_log_data(self):
        datetime = _pd.Timestamp.now()
        run_status = 0 #### TODO: this is not really used right now
        success = self.the_automated_process.no_processed_success
        error = self.the_automated_process.no_processed_error
        warning = self.the_automated_process.no_processed_warning
        subprocess = self.product_name#### TODO: not sure what this was ment for in the early days
        server = socket.gethostname()
        comment = ''
        log_string = f'{datetime},{run_status},{error},{success},{warning},{subprocess},{server},{comment}\n'
        return log_string
    
    def log(self):
        #create file if it does not exist
        file_exists =  self.path2log.is_file()
        
        if not file_exists:
            with open(self.path2log, 'w') as log_out:
                log_out.write('datetime,rund_status,error,success,warning,subprocess,server,comment\n')
        with open(self.path2log, 'a') as log_out:
            log_out.write(self.create_log_data())
        return
    
    # @propety
    def parse_email(self, test = True):
        config = self.settings
        assert(config.get('notify', 'email_address') != 'None'), f'No email has been specified, _please do so in ~/.{self.product_name}/config.ini'
        
        # out = self._last_processing
        # messages = ['run started {}'.format(out['start_time'])]
        messages = ['run started {}'.format(self.start_time)]
        messages.append('run finshed {}'.format(_pd.Timestamp.now()))
        
        #### summary
        # no_of_errors = len(out['errors'])
        # no_of_files_processed = out['no_of_files_processed']
        fail = False

        try:
            if test:
                assert(False), 'sabotage!!!'
            no_of_files_that_need_processing = self.the_automated_process.no_of_files_that_need_processing
            no_of_files_processed = self.the_automated_process.no_of_files_processed
            no_of_errors = self.the_automated_process.no_of_errors
            subject = '{product_name} - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})' 
            msgt = '\n'.join([f'length of workplan:\t\t{no_of_files_that_need_processing}',f'successfully created files:\t{no_of_files_processed}', f'errors encountered:\t\t{no_of_errors}'])

            messages.append(msgt)
                    #### errors
            # if no_of_errors != 0:
            #     errs = out['errors']
            #     err_types = _np.array([err.__class__.__name__ for err in errs])
                
            #     typoferrors = []
            #     for toe in _np.unique(err_types):
            #         typoferrors.append({'name': toe, 'times': (err_types == toe).sum(), 'first_err': errs[_np.where(err_types == toe)[0][0]]})
            
            #     messages.append('\n'.join(['Errors by type:',] + ['\t{tn}:\t{tt}'.format(tn = toe['name'], tt = toe['times']) for toe in typoferrors]))
            #     messages.append('\n=============================================\n'.join(['First traceback for each error type',]+[''.join(_tb.format_tb(toe['first_err'].__traceback__) + [toe['first_err'].__str__(),]) for toe in typoferrors]))
            

            
            #### subject
            if no_of_errors ==0:
                status = 'clean'
            else:
                status = 'errors'
            # subject = f'cl51cloudprod - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})'
            
            subject = subject.format(product_name = self.product_name, status = status, no_of_files_processed = no_of_files_processed, no_of_errors = no_of_errors)
        
        except:                
            fail = _tb.format_exc()
            subject = f'{self.product_name} - status: Fail'
            messages.append(fail)
        
        #### email body
        message_txt = '\n=========================================================================\n'.join(messages)
        msg = _MIMEText(message_txt)
        address  = config.get('notify', 'email_address')
        # smtp = config.get('notify', 'smtp')
        
        msg['Subject'] = subject
        msg['From'] = address
        msg['To'] = address
        return msg
        
        
    def notify(self,
               # product_name,
               # no_of_files_processed,
               # start_time,
               # finish_time,
               # no_of_errors = None
               # subject = 'cl51cloudprod - status: {status} (clean: {no_of_files_processed}; errors: {no_of_errors})',
               ):

        
        # Send the message via our own SMTP server.
        smtp = self.settings.get('notify', 'smtp')
        s = _smtplib.SMTP(smtp)
        s.send_message(self.parse_email())
        s.quit()
    