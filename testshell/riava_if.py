import sys
import pexpect
import ool_rm_if
import time

DBG_FLAG='ON'
CONSOLE_OUT_FLAG='ON'
TRACE_TITLE='RIAVA:%s'
EXCEPT_TITLE='RIAVA-Error:%s'

EXPECT_LOGIN='User:'
EXPECT_PASSWD='Password:'
EXPECT_STDPT='.*> '
EXPECT_MANPT='.*# '
SEND_ENABLE='enable'
SEND_RELOAD='reload'
SEND_YES='y'
SEND_QUIT='quit'
SEND_EXIT='exit'

TIMEOUT=30

#-----------------------------------
class riava_if:

	def __init__(self):
		self.host = ''
		self.ip   =''
		self.uname=''
		self.upw  =''
		self.dname=''
		self.logger=''
		self.auth ='xxx'
		self.bk_uname = ''
		self.bk_upw = ''
		self.bk_uip = ''
		self.priority=''
		self.ofc_ip=[]
		self.sync_mode = 'SYNC'
		self.time_out = 90

		self.ori=ool_rm_if.ool_rm_if()

	def __trace_log__(self, log):
		if 'ON' == DBG_FLAG:
			self.logger.debug(TRACE_TITLE %(log))
		return 0

	def __except_log__(self, log):
		self.logger.debug(EXCEPT_TITLE %(log))
		return 0

	def __get_hw_info__(self):
		self.__trace_log__('get_hw_info IN')

		# get pysical switch info
		self.ori.set_auth(self.auth)
		data=self.ori.get_switch(self.host)

		if -1 != data[0]:
			data1={}
			data1=data[1]
			self.ip   = data1['ip_address']
			self.uname= data1['user_name']
			if True == data1.has_key('password'):
				self.upw  = data1['password']
			else:
				self.upw  = ''
			self.sname= data1['product_name']
			self.mac  = data1['mac_address']
		else:
			self.__trace_log__('get_hw_info error')
			self.__except_log__(data[1])
			return -1

		self.__trace_log__('get_hw_info OUT')
		return 0

	def __get_BKsv_info__(self, bk_host):
		self.__trace_log__('get_BKsv_info IN')

		# get storage server info
		self.ori.set_auth(self.auth)
		node_data=self.ori.get_node(bk_host)
		nic_data=self.ori.get_nic_traffic_info(bk_host, "M-Plane")

		if -1 != node_data[0]:
			node_data1={}
			node_data1=node_data[1]
			self.bk_uid = node_data1['user_name']
			self.bk_upw = node_data1['password']
			nic_data1={}
			nic_data1=nic_data[1][0]
			self.bk_uip = nic_data1['ip_address']
		else:
			self.__except_log__(node_data[1])
			return -1

		self.__trace_log__('get_BKsv_info OUT')
		return 0

	def __expectsendline__(self, spawn, expect, sendline):
		spawn.expect(expect)
		spawn.sendline(sendline)

	def set_auth(self, auth):
		self.auth=auth

	def set_logger(self,logging):
		self.logger=logging

	def set_host_name(self, host):
		self.host=host

	def set_ofc_info(self, priority, ofc_ip):
		self.priority=priority
		self.ofc_ip=ofc_ip

	def set_sync_mode(self, mode, time_out):
		self.sync_mode = mode
		self.time_out = time_out

	def exec_backup(self):
		self.__trace_log__('exec_backup IN')

		try:
		# start backup
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == CONSOLE_OUT_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

			i=c.expect([EXPECT_PASSWD, EXPECT_STDPT])
			if 0 == i :
				c.sendline(self.upw)

			c.sendline(SEND_ENABLE)

			# copy configuration file
			self.__expectsendline__(c, EXPECT_MANPT, 'copy system:running-config nvram:startup-config')
			self.__expectsendline__(c, 'Are you sure you want to save? (y/n)', SEND_YES)

			para='copy nvram:startup-config scp://%s@%s/%s/%s' %(self.bk_uid, self.bk_uip, self.bpath_para, self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, para)
			self.__expectsendline__(c, 'Remote Password:', self.bk_upw)
			self.__expectsendline__(c, 'Are you sure you want to start? (y/n)', SEND_YES)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			c.logfile=sys.stdout
			print str(c)
			return -1

		self.__trace_log__('exec_backup OUT')
		return 0

	def exec_restore(self):
		self.__trace_log__('exec_restore IN')
		ret = 0

		try:
			# start restore
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == CONSOLE_OUT_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

			i = c.expect([EXPECT_PASSWD, EXPECT_STDPT])
			if 0 == i:
				c.sendline(self.upw)

			c.sendline(SEND_ENABLE)

			# copy configuration file
			para='copy scp://%s@%s/%s/%s nvram:startup-config' %(self.bk_uid, self.bk_uip, self.bpath_para, self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, para)

			self.__expectsendline__(c, 'Remote Password:','bpw')
			self.__expectsendline__(c, 'Are you sure you want to start? (y/n)', SEND_YES)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_RELOAD)
			self.__expectsendline__(c, 'Are you sure you would like to reset the system? (y/n)', SEND_YES)

			self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			c.expect(pexpect.EOF)
			c.close()
			ret = 0
		except:
			self.__except_log__(c)
			return -1

		if 'SYNC' != self.sync_mode:
			return ret

		# wait reboot
		time_s = time.time()

		while True:
			time_p = time.time()
			timedf = time_p - time_s
			timedf_int = int(timedf)

			chk_time = (timedf_int % 3600)/60 + timedf_int % 60

			try:
				c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
				if 'ON' == CONSOLE_OUT_FLAG:
					c.logfile=sys.stdout

				self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

				i = c.expect([EXPECT_PASSWD, EXPECT_STDPT])
				if 0 == i:
					c.sendline(self.upw)

				self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

				c.expect(pexpect.EOF)
				c.close()
				ret = 0
				break
			except:
				if chk_time >= self.time_out:
					self.__except_log__(c)
					ret = -1
					break
			continue

		self.__trace_log__('exec_restore OUT')
		return ret

	def set_bksrv(self, bhost, bpath):
		self.__trace_log__('put_conf IN')
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('get_hw info error')
			return -1

		ret=self.__get_BKsv_info__(bhost)
		if -1 == ret:
			self.__trace_log__('BKsv info error')
			return -1

		self.bpath_para=str(bpath)
		return 0
