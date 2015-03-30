import sys
import pexpect
import ool_rm_if
import time

DBG_FLAG='ON'
CONSOLE_OUT_FLAG='ON'
OFC_CHANNEL_PORT='6633'

TRACE_TITLE='CENTEC:%s'
EXCEPT_TITLE='CENTEC-Error:%s'
SRC_DIR='lib/'
SRC_CNF_FILE='startup-config.conf'
SRC_CNF_OPT='.sav'
SRC_OVSDB_FILE='conf_backup.tar.gz'
DST_DIR='/backup/'
DST_OVSDB_OPT='_ovs'

EXPECT_LOGIN='Username:'
EXPECT_PASSWD='Password:'
EXPECT_MANPT='.*# '
SEND_CONFIGURE='configure terminal'
SEND_YES='y'
SEND_REBOOT='reboot'
SEND_EXIT='exit'

VERSION_10=10
VERSION_11=11
VERSION_13=13

TIMEOUT=30

WK_SRC_CONF='/tmp/work_src.conf'
WK_DST_CONF='/tmp/work_dst.conf'

#------------------------------------
class centec_if:

	def __init__(self):
		self.host =''
		self.ip   =''
		self.uname=''
		self.upw  =''
		self.dname=''
		self.logger=''
		self.auth   ='xxx'
		self.bk_uname =''
		self.bk_upw =''
		self.bk_uip =''
		self.priority=''
		self.ofc_ip=[]
		self.port_info=[]
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

	def set_port_info(self, port_info):
		self.port_info=port_info

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

			i = c.expect([EXPECT_PASSWD])
			if 0 == i :
				c.sendline(self.upw)

			# copy configuration file
			self.__expectsendline__(c, EXPECT_MANPT, 'copy running-config startup-config')

			para = 'copy %s%s mgmt-if ftp:// ' % (SRC_DIR, SRC_CNF_FILE)
			para = '%s%s:%s@%s/%s/%s' % (para, self.bk_uid, self.bk_upw, self.bk_uip, self.bpath_para, self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, para)

			para = 'copy %s%s mgmt-if ftp:// ' % (SRC_DIR, SRC_OVSDB_FILE)
			para = '%s%s:%s@%s/%s/%s' % (para, self.bk_uid, self.bk_upw, self.bk_uip, self.bpath_para, self.dname + DST_OVSDB_OPT)
			self.__expectsendline__(c, EXPECT_MANPT, para)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
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

			i = c.expect([EXPECT_PASSWD, EXPECT_MANPT])
			if 0 == i :
				c.sendline(self.upw)

			para ='copy mgmt-if ftp:// %s:%s@%s'  % (self.bk_uid, self.bk_upw, self.bk_uip)
			para ='%s/%s/%s %s%s' % (para, self.bpath_para, self.dname, SRC_DIR, SRC_CNF_FILE)
			c.sendline(para)

			self.__expectsendline__(c, '.*[confirm]', SEND_YES)

			para ='copy mgmt-if ftp:// %s:%s@%s'  % (self.bk_uid, self.bk_upw, self.bk_uip)
			para ='%s/%s/%s %s%s' % (para, self.bpath_para, self.dname + DST_OVSDB_OPT, SRC_DIR, SRC_OVSDB_FILE)
			self.__expectsendline__(c, EXPECT_MANPT, para)
			self.__expectsendline__(c, '.*[confirm]', SEND_YES)
			self.__expectsendline__(c, EXPECT_MANPT, 'start shell')
			self.__expectsendline__(c, 'start shell', SEND_REBOOT)

			c.expect(pexpect.EOF)
			c.close()
			ret = 0
		except:
			self.__except_log__(c)
			return -1

		# wait reboot
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
				c = pexpect.spawn('telnet %s' % (self.ip),  timeout = 5)
				if 'ON' == CONSOLE_OUT_FLAG:
					c.logfile=sys.stdout

				self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

				i = c.expect([EXPECT_PASSWD, EXPECT_MANPT])
				if 0 == i :
					c.sendline(self.upw)

				self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)

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

	def setup_ofc(self):
		# start
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('get_hw info error')
			return -1

		try:
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == DBG_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

			i = c.expect([EXPECT_PASSWD, EXPECT_MANPT])
			if 0==i:
				c.sendline(self.upw)

			# configure openflow
			c.sendline(SEND_CONFIGURE)
			para = 'openflow set controller tcp %s %s' % (self.ofc_ip[0], OFC_CHANNEL_PORT)
			self.__expectsendline__(c, EXPECT_MANPT, para)

			para = 'openflow set protocols openflow%s' % (VERSION_13)
			self.__expectsendline__(c, EXPECT_MANPT, para)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_EXIT)

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
			self.__trace_log__('get_hw info error')
			return -1

		ret=self.__get_BKsv_info__(bhost)
		if -1 == ret:
			self.__trace_log__('BKsv info error')
			return -1

		self.bpath_para=str(bpath)
		self.bpath_para=self.bpath_para.replace(DST_DIR, '')
		self.__trace_log__('put_conf OUT')
		return 0

