import pexpect
import sys
import ool_rm_if

DBG_FLAG='ON'
CONSOLE_OUT_FLAG='ON'
OFC_CHANNEL_PORT='6633'

EXPECT_LOGIN='.*login:'
EXPECT_PASSWD='Password:'
EXPECT_STDPT='.*> '
EXPECT_MANPT='.*# '
SEND_ENABLE='enable'
SEND_CONFIGURE='configure'
SEND_COMMIT='commit'
SEND_LR='\r'
SEND_YES='yes'
SEND_QUIT='quit'
SEND_EXIT='exit'

TIMEOUT=30

TRACE_TITLE='PICA8:%s'
EXCEPT_TITLE='PICA8-Error:%s'

#-----------------------------------
class pica8_if:

	def __init__(self):
		self.host =''
		self.ip   =''
		self.uname=''
		self.upw  =''
		self.dname=''
		self.logger=''
		self.auth ='xxx'
		self.priority=''
		self.ofc_ip=[]
		self.port=[]
		self.bpath_para=''
		self.bk_uname = ''
		self.bk_upw = ''
		self.bk_uip = ''
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

		get_port_data=self.ori.get_port(self.host)

		if -1 != get_port_data[0]:
			self.port=get_port_data[1]
		else:
			print "<port info get error>"
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
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('get_hw info error')
			return -1

		ret=self.__get_BKsv_info__(self.bhost)
		if -1 == ret:
			self.__trace_log__('BKsv info error')
			return -1

		try:
			# start backup
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == CONSOLE_OUT_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

			i = c.expect([EXPECT_PASSWD,EXPECT_STDPT])
			if 0 == i :
				c.sendline(self.upw)

			c.sendline(SEND_CONFIGURE)

			# copy configuration file
			self.__expectsendline__(c, EXPECT_MANPT, 'save ' + self.dname)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_LR)
			c.sendline(SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, 'start shell sh')
			self.__expectsendline__(c, 'Input.*:',self.upw)

			para = 'scp /pica/config/%s/%s %s@%s:%s/%s' %(self.uname, self.dname, self.uname, self.bhost, self.bpath_para, self.dname)
			self.__expectsendline__(c, '.*@XorPlus:.*$',para)

			i = c.expect(['Are you sure you want to continue connecting (yes/no)?','.*\'s password: '])
			if 0 == i:
				c.sendline(SEND_YES)
				self.__expectsendline__(c, '.*\'s password: ',self.bk_upw)
			if 1 == i:
				c.sendline(self.bk_upw)

			self.__expectsendline__(c, '.*@XorPlus:.*$', SEND_EXIT)
			c.sendline(SEND_LR)

			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
			return -1

		self.__trace_log__('exec_backup OUT')
		return 0

	def exec_restore(self):
		self.__trace_log__('exec_restore IN')
		ret=self.__get_hw_info__()
		if -1 == ret:
			self.__trace_log__('get_hw info error')
			return -1

		ret=self.__get_BKsv_info__(self.bhost)
		if -1 == ret:
			self.__trace_log__('BKsv info error')
			return -1

		try:
			# start restore
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == CONSOLE_OUT_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)

			i=c.expect([EXPECT_PASSWD,EXPECT_STDPT])
			if 0==i:
				c.sendline(self.upw)

			# copy configuration file
			self.__expectsendline__(c, EXPECT_STDPT, 'start shell sh')
			self.__expectsendline__(c, 'Input.*:', self.upw)

			para = 'scp %s@%s:%s/%s /pica/config/%s/%s' %(self.bk_uid, self.bhost, self.bpath_para, self.dname, self.uname, self.dname)
			self.__expectsendline__(c, '.*@XorPlus.*$', para)

			i = c.expect(['Are you sure you want to continue connecting (yes/no)?', '.*\'s password: '])
			if 0 == i:
				c.sendline(SEND_YES)
				self.__expectsendline__(c, '.*\'s password: ',self.bk_upw)
			if 1 == i:
				c.sendline(self.bk_upw)

			self.__expectsendline__(c, '.*@XorPlus.*$', SEND_EXIT)
			c.sendline(SEND_LR)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_CONFIGURE)
			self.__expectsendline__(c, EXPECT_MANPT, 'load ' + self.dname)

			i =  c.expect(['.*\'s password: ','Load done.'])
			if 0 == i:
				c.sendline(self.bk_upw)

			c.sendline(SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)
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
			self.__trace_log__('get_hw info error')
			return -1

		try:
			c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
			if 'ON' == DBG_FLAG:
				c.logfile=sys.stdout

			self.__expectsendline__(c, EXPECT_LOGIN, self.uname)
			self.__expectsendline__(c, EXPECT_PASSWD, self.upw)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_CONFIGURE)

			# configure openflow
			self.__expectsendline__(c, EXPECT_MANPT, 'delete open-flow')
			self.__expectsendline__(c, EXPECT_MANPT, SEND_COMMIT)
			self.__expectsendline__(c, EXPECT_MANPT, 'set open-flow allowed-versions openflow-v1.0 disable false')
			self.__expectsendline__(c, EXPECT_MANPT, 'set open-flow controller controller-srv protocol tcp')
			self.__expectsendline__(c, EXPECT_MANPT, 'set open-flow controller controller-srv port %s' % (OFC_CHANNEL_PORT))
			self.__expectsendline__(c, EXPECT_MANPT, SEND_COMMIT)
			self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
			self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			c.expect(pexpect.EOF)
			c.close()
		except:
			self.__except_log__(c)
			return -1

		c=''
		try:
			# start interface's setup
			for i in range(len(self.port)):
				# setup openflow
				if '02' ==self.port[i]['vlan'][i]['vlan_type']:
					c = pexpect.spawn('telnet %s' % (self.ip),  timeout=TIMEOUT)
					if 'ON' == DBG_FLAG:
						c.logfile=sys.stdout

					self.__expectsendline__(c, EXPECT_LOGIN, self.uname)
					self.__expectsendline__(c, EXPECT_PASSWD, self.upw)
					self.__expectsendline__(c, EXPECT_STDPT, SEND_CONFIGURE)

					# configure openflow
					port_name='set interface gigabit-ethernet ge-1/1/' + str(i)

					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' mtu 1514')
					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' speed auto')
					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' disable false')
					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' ether-options flow-control true')
					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' crossflow enable true')
					self.__expectsendline__(c, EXPECT_MANPT, port_name + ' crossflow local-control false')
					self.__expectsendline__(c, EXPECT_MANPT, SEND_COMMIT)

					self.__expectsendline__(c, EXPECT_MANPT, SEND_QUIT)
					self.__expectsendline__(c, EXPECT_STDPT, SEND_QUIT)

			if ''!=c:
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
		return 0


