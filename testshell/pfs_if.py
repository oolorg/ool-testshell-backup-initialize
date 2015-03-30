import sys
import pexpect
import ool_rm_if

DBG_FLAG='ON'
CONSOLE_OUT_FLAG='ON'
CONTROLLER_NAME='ool'
OFC_CHANNEL_PORT='6633'

TRACE_TITLE='PFS:%s'
EXPECT_TITLE='PFS-Error:%s'
DST_DIR='/backup/'

EXPECT_LOGIN='login:'
EXPECT_PASSWD='Password:'
EXPECT_STDPT='.*> '
EXPECT_MANPT='.*# '
SEND_DISABLE='no enable'
SEND_ENABLE='enable'
SEND_CONFIGURE='configure'
SEND_SAVE='save'
SEND_STOPCON='set logging console disable E7'
SEND_STARTCON='set logging console enable'
SEND_YES='y'
SEND_QUIT='quit'
SEND_EXIT='exit'

TIMEOUT=30
WAIT_CNT=10

WK_SRC_CONF='/tmp/work_src.conf'
WK_DST_CONF='/tmp/work_dst.conf'

#-----------------------------------
class pfs_if:

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
		self.port_info=[]
		self.bpath_para=''
		self.sync_mode = 'SYNC'
		self.time_out = 90

		self.ori=ool_rm_if.ool_rm_if()

	def __trace_log__(self, log):
		if 'ON' == DBG_FLAG:
			self.logger.debug(TRACE_TITLE %(log))
		return 0

	def __except_log__(self, log):
		self.logger.debug(EXPECT_TITLE %(log))
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
			self.dname= data1['device_name']
			self.dname=str(self.dname).replace(':', '_')
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
			self.bk_uname = node_data1['user_name']
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
		try:
			spawn.expect(expect,  timeout=TIMEOUT)
			spawn.sendline(sendline)
			return 0
		except:
			return -1

	def set_auth(self, auth):
		self.auth=auth

	def set_logger(self,logging):
		self.logger=logging

	def set_host_name(self, host):
		self.host=host

	def set_ofc_info(self, priority, ofc_ip):
		self.priority=priority
		self.ofc_ip=ofc_ip

	def set_port_info(self, port_info):
		self.port_info=port_info

	def set_sync_mode(self, mode, time_out):
		self.sync_mode = mode
		self.time_out = time_out

	def exec_backup(self):
		self.__trace_log__('exec_backup IN')

		try:
			# start backup
			for i in range(1, WAIT_CNT):
				c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
				if 'ON' == CONSOLE_OUT_FLAG:
					c.logfile=sys.stdout

				ret = self.__expectsendline__(c, EXPECT_LOGIN, self.uname)
				if 0 == ret:
					break
				c.close
				continue

			i = c.expect([EXPECT_PASSWD])
			if 0 == i:
				c.sendline(self.upw)

			c.sendline(SEND_STOPCON)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_ENABLE)

			i = c.expect([EXPECT_PASSWD])
			if 0==i :
				c.sendline(self.upw)

			# copy configuration file
			para='copy running-config ftp://%s@%s:/%s/%s' %(self.bk_uname, self.bk_uip, self.bpath_para, self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, para)

			i = c.expect(['Configuration file copy to.*', EXPECT_PASSWD])
			if 0 == i:
				c.sendline(SEND_YES)
				self.__expectsendline__(c, EXPECT_PASSWD, self.bk_upw)
			if 1==i:
				c.sendline(self.bk_upw)

			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_STARTCON)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_EXIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
			return -1

		self.__trace_log__('exec_backup OUT')
		return 0

	def exec_restore(self):
		self.__trace_log__('exec_restore IN')

		try:
			# start restore
			for i in range(1,WAIT_CNT):
				c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
				if 'ON' == CONSOLE_OUT_FLAG:
					c.logfile=sys.stdout

				ret = self.__expectsendline__(c, EXPECT_LOGIN, self.uname)
				if 0 == ret:
					break
				c.close
				continue

			i = c.expect([EXPECT_PASSWD])
			if 0 == i:
				c.sendline(self.upw)

			c.sendline(SEND_STOPCON)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_ENABLE)

			i = c.expect([EXPECT_PASSWD])
			if 0 == i :
				c.sendline(self.upw)

			# copy configuration file
			para = 'copy ftp://%s@%s:/%s/%s running-config' %(self.bk_uname, self.bk_uip, self.bpath_para, self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, para)
			c.sendline(SEND_YES)

			i = c.expect(['Configuration file copy to .* (y/n): ',EXPECT_PASSWD])
			if 0==i:
				c.sendline('y')
				self.__expectsendline__(c, EXPECT_PASSWD, self.bk_upw)
			if 1==i:
				c.sendline(self.bk_upw)

			c.expect(EXPECT_MANPT, timeout=180)
			c.sendline('copy running-config startup-config')

			#c.expect('Configuration file copy to .*(y/n):')
			c.sendline('y')

			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_STARTCON)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_EXIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
			return -1

		self.__trace_log__('exec_restore OUT')
		return 0

	def setup_ofc(self):
		# start
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('settup_ofc:get_hw info error')
			return -1

		try:
			for i in range(1,WAIT_CNT):
				c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
				if 'ON' == CONSOLE_OUT_FLAG:
					c.logfile=sys.stdout

				ret = self.__expectsendline__(c, EXPECT_LOGIN, self.uname)
				if 0 == ret:
					break
				c.close
				continue

			i = c.expect([EXPECT_PASSWD])
			if 0==i:
				c.sendline(self.upw)

			c.sendline(SEND_STOPCON)

			self.__expectsendline__(c, EXPECT_STDPT, SEND_ENABLE)

			i = c.expect([EXPECT_PASSWD])
			if 0 == i:
				c.sendline(self.upw)

			# configure openflow
			self.__expectsendline__(c, EXPECT_MANPT, SEND_CONFIGURE)
			self.__expectsendline__(c, EXPECT_MANPT, 'no openflow openflow-id 1')

			i = c.expect(['.*is not corresponding.','.*openflow-id 1'])
			if 1==i:
				j = c.expect(['.*is not corresponding.',EXPECT_MANPT])
				if 0 == j:
					print '0'
				if 1 == j:
					print '1'

			c.sendline('openflow openflow-id 1')
			self.__expectsendline__(c, EXPECT_MANPT, SEND_DISABLE)

			for i in range(len(self.ofc_ip)):
				para='controller controller-name %s %s %s port %s tcp' %(CONTROLLER_NAME, str(int(self.priority) - i), self.ofc_ip[i], OFC_CHANNEL_PORT)
			self.__expectsendline__(c, EXPECT_MANPT, para)

			c.expect(EXPECT_MANPT)
			c.sendline('miss-action controller')

			self.__expectsendline__(c, EXPECT_MANPT, SEND_ENABLE)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_SAVE)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_STARTCON)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
			return -1

		return 0

	def set_bksrv(self, bhost, bpath):
		self.__trace_log__('put_conf IN')
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('set_bksrv:get_hw info error')
			return -1

		ret=self.__get_BKsv_info__(bhost)
		if -1 == ret:
			self.__trace_log__('BKsv info error')
			return -1

		self.bpath_para=str(bpath)
		self.bpath_para=self.bpath_para.replace(DST_DIR, '')
		return 0

