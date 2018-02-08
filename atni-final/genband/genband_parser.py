import binascii
import gzip
import os
from array import array
from datetime import datetime, timedelta
import re
import traceback
import sys




def jdefault(o):
    return o.__dict__

class Parser:

    __structure_codes={
        # Structure code x0510 is used for all('call generated' Bellcore AMA billing
        '0510':(
            ('TOTAL_SIZE',134),
            ('Call Type Code',4),
            ('Sensor Type',4),
            ('Sensor Identification',8),
            ('Recording Office Type',4),
            ('Recording Office Identification',8),
            ('Date',6),
            ('Timing Indicator',6),
            ('Study Indicator',8),
            ('Called Party Off-Hook',2),
            ('Service Observed, Traffic Sampled',2),
            ('Operator Action',2),
            ('Service Feature',4),
            ('Originating Significant Digits',4),
            ('Originating Open Digits 1',12),
            ('Originating Open Digits 2',10),
            ('Originating Charge Information',4),
            ('Domestic/International Indicator',2),
            ('Terminating Significant Digits',4),
            ('Terminating Open Digits 1',12),
            ('Terminating Open Digits 2',10),
            ('Connect Time',8),
            ('Elapsed Time',10)
        ),

        # Structure code x0514 is used for all('call generated' Bellcore AMA billing
        '0514':(
            ('TOTAL_SIZE',148),
            ('Call Type Code',4),
            ('Sensor Type',4),
            ('Sensor Identification',8),
            ('Recording Office Type',4),
            ('Recording Office Identification',8),
            ('Date',6),
            ('Timing Indicator',6),
            ('Study Indicator',8),
            ('Answer Indicator',2),
            ('Service Observed, Traffic Sampled',2),
            ('Operator Action',2),
            ('Service Feature',4),
            ('Originating Significant Digits',4),
            ('Originating Open Digits 1',12),
            ('Originating Open Digits 2',10),
            ('Originating Charge Information',4),
            ('Domestic/International Indicator',2),
            ('Terminating Significant Digits',4),
            ('Terminating Open Digits 1',16),
            ('Terminating Open Digits 2',16),
            ('Connect Time',8),
            ('Elapsed Time',10),
            ('Completion Indicator',4)
        ),

        # Operator assisted calls
        '0106':(
            ('TOTAL_SIZE',114),
            ('Call type code',4),
            ('Sensor type',4),
            ('Sensor identification',8),
            ('Recording office type',4),
            ('Recording office identification',8),
            ('Date',6),
            ('Timing indicator',6),
            ('Study Indicator',8),
            ('Service observed, traffic sampled',2),
            ('Significant digits in next field',4),
            ('Originating open digits 1',12),
            ('Originating open digits 2',10),
            ('Originating charge information',4),
            ('Time',8),
            ('Elapsed time',10),
            ('Service feature',4),
            ('Station signaling indicator',2),
            ('Screening code',4),
            ('Called number/service access number input',2),
            ('Calling number source',2)
        ),

        # The following structure is used at the start of any new file
        '9013':(
            ('TOTAL_SIZE',56),
            ('Call Type Code',4),
            ('Sensor Type',4),
            ('Sensor Identification',8),
            ('Recording Office Type',4),
            ('Recording Office Identification',8),
            ('Date',6),
            ('Time',8),
            ('Recorder Generic Issue',6),
            ('Type Of Tracer',4),
            ('Sequence Number',4)
        ),

        # The following structure is placed at the end of a file before closing it
        '9014':(
            ('TOTAL_SIZE',70),
            ('Call Type Code',4),
            ('Sensor Type',4),
            ('Sensor Identification',8),
            ('Recording Office Type',4),
            ('Recording Office Identification',8),
            ('Date',6),
            ('Time',8),
            ('Recorder Generic Issue',6),
            ('Type Of Tracer',4),
            ('Sequence Number',4),
            ('Record Count',8),
            ('Block Count',6)
        )
    }

    __module_codes={
        # Long Duration Call Module code
        '022':(
            ('TOTAL_SIZE',14),
            ('Present Date',6),
            ('Present Time',8)
        ),

        # Wide area telecommunications service (WATS)
        '023':(
            ('TOTAL_SIZE',12),
            ('WATS Indicator',2),
            ('WATS Band Or MBG',4),
            ('WATS Administration',6)
        ),

        # Wide area telecommunications service (WATS)
        '024':(
            ('TOTAL_SIZE',12),
            ('Service Indicator',4),
            ('Data rate Indicator',4),
            ('Terminating Company',4)
        ),

        # Circuit Release Module code
        '025':(
            ('TOTAL_SIZE',14),
            ('Circuit Release Date',6),
            ('Circuit Release Time',8)
        ),

        # VPN Module code
        '026':(
            ('TOTAL_SIZE',28),
            ('Significant Digits in Next Field',4),
            ('Digits Dialed or Additional Digits Dialed',16),
            ('NPA-NXX',8)
        ),

        # VPN Module code
        '027':(
            ('TOTAL_SIZE',12),
            ('Business Customer Identification',12),
        ),

        # Additional Digits Dialed Code
        '028':(
            ('TOTAL_SIZE',20),
            ('Significant Digits in Next Field',4),
            ('Digits Dialed or Additional Digits Dialed',16)
        ),

        # Alternate billing number
        '029':(
            ('TOTAL_SIZE',12),
            ('Alternate Billing Number',12)
        ),

        # Translation Settable Code
        '030':(
            ('TOTAL_SIZE',8),
            ('Context ID',4),
            ('Translation Settable Field',4)
        ),

        # Feature type indicator
        '031':(
            ('TOTAL_SIZE',14),
            ('Feature Type Indicator',14),
        ),

        # Operator information
        '032':(
            ('TOTAL_SIZE',20),
            ('Operator identifications',14),
            ('Accumulated operator work time',6)
        ),

        # Intl call completion service
        '033':(
            ('TOTAL_SIZE',46),
            ('Domestic/international indicator',2),
            ('Significant digits in next field',4),
            ('Terminating open digits 1',12),
            ('Terminating open digits 2',10),
            ('Completion indicator',4),
            ('Rate indicator',2),
            ('OSS call completion service conditions',8),
            ('Alternate route number',4)
        ),

        # ONPE charges
        '036':(
            ('TOTAL_SIZE',34),
            ('Charge indicator',2),
            ('Base charge',6),
            ('Tax',6),
            ('Surcharge',6),
            ('Amount deposited',6),
            ('Multiplier factor',6),
            ('Coin credit indicator',2)
        ),

        # ONPE line number
        '037':(
            ('TOTAL_SIZE',32),
            ('Line number type',4),
            ('Domestic/international indicator',2),
            ('Line number Significant Digits',4),
            ('Line number open digits 1',12),
            ('Line number open digits 2',10)
        ),

        # Digits Module code
        '040':(
            ('TOTAL_SIZE',34),
            ('Digits Identifier',4),
            ('Significant Digits in Next Field',4),
            ('Dialed Digits 1',12),
            ('Dialed Digits 2',14)
        ),

        # Call Record Sequence Module code
        '042':(
            ('TOTAL_SIZE',46),
            ('Call Record Sequence',8),
            ('Digits Module code 040',4),
            ('Digits Identifier',4),
            ('Significant Digits in Next Field',4),
            ('Dialed Digits 1',12),
            ('Dialed Digits 2',14)
        ),

        # Call Record Sequence Module code
        '042':(
            ('TOTAL_SIZE',8),
            ('Call Record Sequence',8)
        ),

        # Alternate Billing Number Module code
        '046':(
            ('TOTAL_SIZE',28),
            ('Source of Charge Number',2),
            ('Significant Digits in Next field',4),
            ('Originating Open Digits 1',12),
            ('Originating Open Digits 2',10)
        ),

        # ISDN Core Module code
        '070':(
            ('TOTAL_SIZE',28),
            ('Bearer Capabilities',4),
            ('Network Interworking',2),
            ('Signaling or Supplementary Service Capabilities Use',16),
            ('Release Cause Indicator',6)
        ),

        # ISDN Abbreviated Core Module code
        '071':(
            ('TOTAL_SIZE',12),
            ('Bearer Capabilities',4),
            ('Network Interworking',2),
            ('Release Cause Indicator',6)
        ),

        # Terminating User Service Module
        '073':(
            ('TOTAL_SIZE',26),
            ('ISDN signaling or supplementary service capability',16),
            ('Interexchange carrier',6),
            ('Bearer capability / call type',4)
        ),

        # Carrier Connect Time Module code
        '098':(
            ('TOTAL_SIZE',16),
            ('Carrier Connect Date',6),
            ('Carrier Connect Time',8),
            ('Message Direction',2)
        ),

        # MDR Module code
        '100':(
            ('TOTAL_SIZE',60),
            ('Business Customer Identification',12),
            ('Call Completion Code',2),
            ('Business Feature Code',2),
            ('Automatic Route Selection Pattern',2),
            ('Facility Restriction Level',2),
            ('Incoming Facility Type',4),
            ('Incoming Trunk Facility Identification',10),
            ('Outgoing Facility Type',4),
            ('Outgoing Trunk Facility Identification',10),
            ('End Of Dialing Time',8),
            ('Queue Elapsed Time',4)
        ),

        # AUTHCODE in MDR Module code
        '102':(
            ('TOTAL_SIZE',20),
            ('Significant Digits in Next Field',4),
            ('Authorization Code',16)
        ),

        # Account Code Module code
        '103':(
            ('TOTAL_SIZE',20),
            ('Significant Digits in Next Field',4),
            ('Account Code/CDAR',16)
        ),

        # Trunk Identification Module code
        '104':(
            ('TOTAL_SIZE',10),
            ('Trunk Identification Number',10)
        ),

        # This module contains the information required to calculate the time taken to answer a call
        '115':(
            ('TOTAL_SIZE',44),
            ('Context Identifier',4),
            ('Significant Digits in Next Field',4),
            ('Terminating Open Digits 1',12),
            ('Terminating Open Digits 2',10),
            ('Date',6),
            ('Time',8)
        ),

        # This module contains any redirection information on a call
        '116':(
            ('TOTAL_SIZE',42),
            ('Call Redirection Indicator',6),
            ('Significant Digits in Next Field',4),
            ('Digits 1',16),
            ('Digits 2',16)
        ),

        # Customer ID Module code
        '120':(
            ('TOTAL_SIZE',6),
            ('Customer Group Identification',6)
        ),

        # Termination Attributes Module code
        '130':(
            ('TOTAL_SIZE',10),
            ('Facility Release Cause',6),
            ('Call Characteristi',4)
        ),

        # This module is the E.164/X.121 number module
        '164':(
            ('TOTAL_SIZE',28),
            ('Number identity',2),
            ('Country code or data network identification code',6),
            ('Significant digits in the next field',4),
            ('Digits',16)
        ),

        # BRI/PRI Channel Identifier Module code
        '180':(
            ('TOTAL_SIZE',6),
            ('ISDN Channel Identifier',6)
        ),

        # BRI/PRI trunk Identifier Module code
        '181':(
            ('TOTAL_SIZE',10),
            ('Trunk Identifcation Number',10)
        ),

        # Furnish Charge Info Module code
        '199':(
            ('TOTAL_SIZE',44),
            ('Data Descriptor',4),
            ('Network Operator Data',40)
        ),

        # OLIP Module code
        '306':(
            ('TOTAL_SIZE',4),
            ('Originating Information Parameter',4)
        ),

        # Time Change Module code
        '504':(
            ('TOTAL_SIZE',38),
            ('Time Before Change',8),
            ('Time After Change',8),
            ('Date Before Change',6),
            ('Date After Change',6)
        ),

        # This module contains the originating and terminating feature codes
        '509':(
            ('TOTAL_SIZE',8),
            ('Originating Feature Code',4),
            ('Terminating Feature Code',4)
        ),

        # Trunk name identifier Module code
        '513':(
            ('TOTAL_SIZE',42),
            ('Trunk CLLI identifier',32),
            ('Trunk identification number',10)
        ),

        # This is a generic, general purpose module
        '611':(
            ('TOTAL_SIZE',24),
            ('Generic context identifier',8),
            ('Digits String 1',16)
        ),

        # This is a generic, general purpose module
        '612':(
            ('TOTAL_SIZE',40),
            ('Generic context identifier',8),
            ('Digits String 1',16),
            ('Digits String 2',16)
        ),

        '613':(
            ('TOTAL_SIZE',24),
            ('Generic context identifier',8),
            ('Digits String 1',16),
            ('Digits String 2',16),
            ('Digits String 3',16)
        ),

        # Listing service
        '055':(
            ('TOTAL_SIZE',32),
            ('Service identification',4),
            ('Means of information input',2),
            ('Means of LSDB access',2),
            ('LSDBs BOC identification',6),
            ('LSDB accesses',2),
            ('Listing response',2),
            ('Listing status',10),
            ('Request counter',4)
        ),

        # Operator keying action
        '310':(
            ('TOTAL_SIZE',10),
            ('Keying actions',10)
        ),

        # Origination call type
        '311':(
            ('TOTAL_SIZE',4),
            ('Originating call type',4)
        ),

        # Local determination
        '316':(
            ('TOTAL_SIZE',2),
            ('Local determination indicator',2)
        ),

        # General assistance service
        '057':(
            ('TOTAL_SIZE',20),
            ('Service identification',4),
            ('Database queried',4),
            ('Means of information input',2),
            ('Request count',4),
            ('Company identification',6)
        ),

        # Notify/opr assist calling card w/DDD
        '062':(
            ('TOTAL_SIZE',4),
            ('Rate indicator',2),
            ('Operator notification',2)
        ),

        # Overwritten number
        '315':(
            ('TOTAL_SIZE',30),
            ('Significant digits in next two fields',4),
            ('Billable digits 1',12),
            ('Billable digits 2',10),
            ('Overwritten number type',2),
            ('Overwritten number sequence',2)
        ),

        # Alternate billing service
        '052':(
            ('TOTAL_SIZE',54),
            ('Billing type identification',2),
            ('Format identifier',2),
            ('Significant digits',4),
            ('Billable digits 1',12),
            ('Billable digits 2',10),
            ('Revenue accounting office (RAO) number',4),
            ('Calling card subaccount number',4),
            ('Billing number treatment number',2),
            ('LIDB response',4),
            ('Operator services system action',2),
            ('Means of input/response',4),
            ('Sequence call counter',4)
        ),

        # Person handling
        '050':(
            ('TOTAL_SIZE',6),
            ('Chargeable operator holding time',6)
        ),

        # Calling/called names and the memo
        '194':(
            ('TOTAL_SIZE',68),
            ('Type of text',4),
            ('Text',64))
    }

    # Private variables
    __file=None
    __fileContent=None
    __structure_size=0
    __modules_present=False
    __position_in_block=0
    __block_data=''
    __lastIndex=0

    def strbin(self, s):
        return ''.join(format(ord(i),'0>8b') for i in s)

    def __init__(self,file, content):
        """Class constructor. Supports compressed(GZIP) or uncompressed OCC files"""
        #if file[-3:].lower()=='.gz':
        #    self.__file=gzip.open(file,'rb')
        #else:
        #    self.__file=open(file,'rb')
        self.__fileContent=content

    def __del__(self):
        """Class destructor"""
        if self.__file: self.__file.close()

    def __iter__(self):
        """Used to support iteration with the 'next' method"""
        return self

    def debug(self):
        # Do some debugging
        print("\n\nData block:")
        print("-----------")
        print self.__block_data
        print("Exploded data block:")
        print("--------------------")
        cnt1=6
        try:
            for l1 in self.__structure_codes[self.__block_data[1:5]]:
                if l1[0]!='TOTAL_SIZE':
                    print("%s = %s" % (l1[0],self.__block_data[cnt1:cnt1+l1[1]]))
                    cnt1=cnt1+l1[1]
        except:
            print("Unrecognized structure code!")

    ## Program origincally coded to use '0510'
    def next(self,structure_code='0514'):
        """Returns the next AMA record as a dictionary. Will return empty dictionary if error encountered or will raise StopIteration once end of file is met"""
        # Reset module_present variable
        self.__modules_present=False
        # Initialize read buffer
        rb=array('c','00000')
        # Search for next block
        i = self.__lastIndex
        test_data = self.__fileContent #self.__file.read()
        data = None
        while True:
            #data=self.__file.read(1)
            # Shift read buffer and append new byte to end
            i=i+1
            self.__lastIndex=i
            try:
                data=test_data[i]
            except:
                raise StopIteration

            if data:
                rb[0],rb[1],rb[2],rb[3],rb[4]=rb[1],rb[2],rb[3],rb[4],data
            if data=='\xAA':
                # Possibly the start of a new block
                hr_data=binascii.hexlify(rb.tostring())
                #print hr_data
                if hr_data[-6:]=='0000aa':
                    # A block was found! Note: 30300000aa is a workaround for an insignificant bug in the AMA files
                    if hr_data=='30300000aa':
                        #global i
                        s=i
                        i=s+86
                        e=i
                        self.__block_data=binascii.hexlify(test_data[s:e])
                    else:
                        #global i
                        s=i+1
                        i=s+(int(hr_data[2:4],16)-5)
                        e=i
                        #print "Here is i %s" %i
                        self.__block_data=binascii.hexlify(test_data[s:e])

                    # Are there modules attached to this block?
                    if self.__block_data[0]=='4' or self.__block_data[0]=='6': self.__modules_present=True
                    else: self.__modules_present=False
                    # Does this block have the structure code being saught?
                    if self.__block_data[1:5]==structure_code:
                        # Extract data for this structure code and return as a dictionary
                        ret_data={}
                        self.__position_in_block=6
                        for (x,y) in self.__structure_codes[structure_code]:
                            if x=='TOTAL_SIZE':
                                self.__structure_size=y
                            else:
                                ret_data[x]=self.__get_chunk(self.__position_in_block,self.__position_in_block+y)
                                self.__position_in_block=self.__position_in_block+y
                        # Got data from block! Position at the end of the block (beginning of first module if present)
                        self.__position_in_block=self.__structure_size+6
                        # Return data retrieved from block (a dictionary of values)
                        return(ret_data)
            elif not data:
                # Stop iteration if end of file is reached
                raise StopIteration

    def reset_module_pointer(self):
        """Will re-position file pointer to the start of the first module attached to last block found"""
        self.__position_in_block=self.__structure_size+6

    def reset_file_pointer(self,file_position=0):
        """Reset file pointer to the beginning of the OCC file by default (unless a different file position is specified)"""
        self.__file.seek(file_position)

    def next_module(self,module_code='104'):
        """Search for the specified module. If found, a dictionary will be returned"""
        if self.__modules_present:
            while True:
                current_module_code=self.__block_data[self.__position_in_block:self.__position_in_block+3]
                if module_code==current_module_code:
                    self.__position_in_block=self.__position_in_block+4
                    ret_data={}
                    for (x,y) in self.__module_codes[module_code]:
                        if x!='TOTAL_SIZE':
                            ret_data[x]=self.__get_chunk(self.__position_in_block,self.__position_in_block+y)
                            self.__position_in_block=self.__position_in_block+y
                    return ret_data
                elif current_module_code!='000':
                    self.__position_in_block=self.__position_in_block+self.__module_codes[current_module_code][0][1]+4
                else:
                    break

    def __get_chunk(self,start,end):
        """Supporting method that will return a specified part of the last retrieved block of data"""
        data=''
        for x in self.__block_data[start:end]:
            if x!='f' and x!='c':
                data=data+x
        return data


# ------------------------------ parse file --------------------------------------
#TODO: Return very basic information stating how many good records and bad records (can use ETL to validate returned data)
def parseFile(fileInfo):
    # acquire the filename for the source file
    src_file_name = fileInfo['filename']
    #print("Parsing: %s -> %s" % (src_file, dst_file))
    cdr_file = Parser('/%s/in/%s' %  (fileInfo['file_path'],fileInfo['filename']))
    threshold_datetime = datetime(fileInfo['dateTime'].year, fileInfo['dateTime'].month, fileInfo['dateTime'].day) + timedelta(days=178)
    # Continue
    records = []
    record_count = [0, 0]
    stats_total = [0, 0]
    for x in cdr_file:
        try:
            rec = {}
            # add file id
            rec['file_id']=fileInfo['id']
            # Calculate 'connect_date' and 'connect_time'
            cdr_month = int(x['Date'][1:3])
            cdr_day = int(x['Date'][3:5])
            cdr_hour = int(x['Connect Time'][0:2])
            cdr_min = int(x['Connect Time'][2:4])
            cdr_sec = int(x['Connect Time'][4:6])
            cdr_datetime = datetime(fileInfo['dateTime'].year, cdr_month, cdr_day, cdr_hour, cdr_min, cdr_sec)
            if cdr_datetime > threshold_datetime:
                cdr_datetime = datetime(fileInfo['dateTime'].year - 1, cdr_month, cdr_day, cdr_hour, cdr_min, cdr_sec)
            rec['connect_datetime'] = str(cdr_datetime)
            rec['connect_date'] = str(cdr_datetime.date())
            rec['__cdr_date'] = rec['connect_date']   # Mandatory field!
            rec['connect_time'] = str(cdr_datetime.time())
            # Determine 'originating_number'
            orig_digits = int(x['Originating Significant Digits'])
            if orig_digits < 12: rec['originating_number'] = x['Originating Open Digits 1'][-orig_digits:]
            else: rec['originating_number'] = x['Originating Open Digits 1'] + x['Originating Open Digits 2'][11 - orig_digits:]
            # Determine 'terminating_number'
            term_digits = int(x['Terminating Significant Digits'])
            if term_digits < 16: rec['terminating_number'] = x['Terminating Open Digits 1'][-term_digits:]
            else: rec['terminating_number'] = x['Terminating Open Digits 1'] + x['Terminating Open Digits 2'][15 - term_digits:]
            # Calculate elapsed time correctly as decimal
            rec['elapsed_time'] = int(x['Elapsed Time'][1:6]) * 60 + float(x['Elapsed Time'][6:]) / 10
            # Get dom/int indicator
            rec['dom_int_indicator'] = x['Domestic/International Indicator']
            # Get first module (trunk ID)
            mod1 = cdr_file.next_module()
            if mod1: rec['trunk_id_1'] = mod1['Trunk Identification Number']
            else: rec['trunk_id_1'] = None
            # Get second module (trunk ID)
            mod2 = cdr_file.next_module()
            if mod2: rec['trunk_id_2'] = mod2['Trunk Identification Number']
            else: rec['trunk_id_2'] = None
            rec['call_code'] = x['Call Type Code']
            rec['completion_ind'] = x['Completion Indicator']
            rec['answer_ind'] = x['Answer Indicator']
            records.append(rec)
            record_count[0] += 1
        except:
            print traceback.print_exc()
            record_count[1] += 1
    # -- Import (send all records to the importer)! --
    #stats = importer.import_records(file_id, records, record_count, db_cleanup)
    #stats_total[0] += stats[0]
    #stats_total[1] += stats[1]
    #print(" --> parsed good records = %d, errors = %d" % (record_count[0], record_count[1]))
    #print(" --> records successfully imported into database = %d, errors = %d\n" % (stats_total[0], stats_total[1]))
    filename_parts=fileInfo['filename'].split('.',1)
    filename=filename_parts[0]
    # write to file
    dstFilePtr = open('%s/process/parsed_%s' % (fileInfo['file_path'],filename),'w')
    for record in records:
        if not record['trunk_id_1']: record['trunk_id_1']=''
        if not record['trunk_id_2']: record['trunk_id_2']=''
        dstFilePtr.write("%(file_id)d|%(connect_date)s %(connect_time)s|%(originating_number)s|%(terminating_number)s|%(elapsed_time).1f|%(dom_int_indicator)s|%(trunk_id_1)s|%(trunk_id_2)s|%(call_code)s|%(completion_ind)s|%(answer_ind)s|\n" % record)
    dstFilePtr.close()
    return record_count

