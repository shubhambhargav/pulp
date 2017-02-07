from __future__ import division
import os
from datetime import datetime
from importlib import import_module

from pulp.utils.mailer import Mailer
from pulp.conf import settings


class ErrorHandler(object):

    def __init__(self):
        """Initialization"""

    def send_error_mail(self, error_logs, subject, recipients):
        if not error_logs:
            return

        message = self.format_error(error_logs)

        Mailer().send_mail(subject, message, recipients)


    def _aggregate_errors(self, error_logs):
        """
        Aggregates the errors list into a dict with error_type and error_count
        """
        d = {}
        d_describe = {}

        if not error_logs:
            return None, None

        for error in error_logs:
            try:
                # fetch error code from the given error
                if len(error.args) == 1:
                    error_code = str(type(error))
                    error_desc = error.args[0]
                else:
                    error_code = error.args[0]
                    error_desc = error.args[1]
                
                if not error_code in d:
                    d[error_code] = 1

                    # add the first description of error code to generalize the final output
                    d_describe[error_code] = error_desc
                else:
                    # increment the already existing error code
                    d[error_code] += 1
            except Exception as ex:
                raise ex
        
        return d, d_describe

    # mail_formatter
    def format_error(self, error_logs, top_val=5):
        """
        Creates message to send in the error mailer
        """
        d_val, d_describe = self._aggregate_errors(error_logs)

        if not d_val:
            return
        
        _msg = ''
        _total = 0
        _top5_ctr = 0

        try:
            for key, val in d_val.iteritems():
                _total += val
            
            _msg += 'Total Errors: %s' % (_total,)
            _msg += '\nTop 5:\n'
            
            for _ in range(top_val):
                if not d_val:
                    break
                # Get maximum key value
                max_key = max(d_val, key=d_val.get)
                max_val = d_val[max_key]

                val_desc = d_describe[max_key]

                # append corresponding string to the message
                _msg += str(max_key) + ': ' + str(val_desc) + ':\t' + str('%.3f'%(max_val*100/_total)) + ' %'
                _msg += '\n'

                # pop the already used value
                _top5_ctr += d_val.pop(max_key)

            # add all the remaining as others
            others_str =  'OTHERS: ' + str(_total - _top5_ctr) + '\n'
            
            # time at which the ingestion is completed
            time_str = 'ts: ' + str(datetime.now())

            """Total processed capability has been removed until further requirement"""
            # total processed values
            #line_processed = 'Total Processed: ' + str(total_processed)

            # total ingested in percentage
            #per_ingested = 'Total Ingested: ' + ('%.3f'%(100 - float(_total*100/total_processed))) + ' %'

            _msg = '\n'.join((_msg, others_str, time_str))#, line_processed, per_ingested))
        except Exception as ex:
            # if aggregation fails because of any string formatting send aggregation failure
            _msg = 'Aggregation Failed: %s' % (ex, )
        
        return _msg