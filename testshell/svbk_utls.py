import ool_rm_if
import logging
import subprocess

SRC_DIR='/etc/ool_br_rest'
DST_DIR='/boot'
BR_AGENT='br_agent_update'

BR_ORG_UPDATE='br.org_update'
BR_ORG='br.org'

OK=0
NG=-1

class svbk_utls:

	def __init__(self):
		self.auth=''
		self.ori=ool_rm_if.ool_rm_if()
		logger = logging.getLogger('utlslog')
		logger.setLevel(logging.DEBUG)
		handler = logging.handlers.SysLogHandler(address = '/dev/log')
		logger.addHandler(handler)
		self.logger=logger

	def _logout(self, msg):
		self.logger.debug('BRLOG msg:%s' %(msg))

	def set_auth(self, auth):
		self.auth=auth

	def set_logging(self, logging):
		self.logger = logging

	def separate_node(self, node_list):
		if False == isinstance(node_list, list):
			return [NG, NG]

		self.ori.set_auth(self.auth)
		server_list=[]
		node_data = self.ori.get_node_all()
		if -1 != node_data[0]:
			data1={}
			tmp_list=''
			data1=node_data[1]
			for i in range(len(data1)):
				tmp_list=tmp_list + data1[i] + ","
			for chk_node in node_list:
				if chk_node in tmp_list:
					server_list.append(chk_node)
		else:
			self._logout("separate_node:<server get error:%s>" % (node_data[1]))
			print "separate_node:<server get error:%s>" % (node_data[1])
			return[NG,OK]

		switch_list=[]
		switch_data = self.ori.get_switch_all()
		if -1 != switch_data[0]:
			data1={}
			tmp_list=''
			data1=switch_data[1]
			for i in range(len(data1)):
				tmp_list=tmp_list + data1[i] + ","
			for chk_switch in node_list:
				if chk_switch in tmp_list:
					switch_list.append(chk_switch)
		else:
			self._logout("separate_node:<switch get error:%s>" % (switch_data[1]))
			print "<device get error:"+ switch_data[1] + ">"
			return[OK,NG]

		return[server_list, switch_list]

	def update_br_agent(self, dst_usr, dst_ip):
		self._logout('#### update %s' % (BR_AGENT))
		msg='success'

		#copy br_agent
		cmd='scp %s/%s %s@%s:%s' %(SRC_DIR, BR_AGENT, dst_usr, dst_ip, DST_DIR)
		print cmd
		ret = self.shellcmd_exec(dst_usr, cmd)

		if NG == ret[0]:
			self._logout('#### update %s err' % (BR_AGENT))
			msg='update agent err'
			return [NG, msg]
		return[OK, msg]

	def update_br_org(self, dst_usr, dst_ip):
		self._logout('#### update %s' % (BR_ORG_UPDATE))
		msg='success'

		#copy br_org
		cmd='scp %s/%s %s@%s:%s/%s' %(SRC_DIR, BR_ORG_UPDATE, dst_usr, dst_ip, DST_DIR,BR_ORG)
		print cmd
		ret = self.shellcmd_exec(dst_usr, cmd)

		if NG == ret[0]:
			self._logout('#### update %s err' % (BR_ORG_UPDATE))
			msg='update br_org err'
			return [NG, msg]
		return[OK, msg]


	def shellcmd_exec(self, exec_user, cmd):
		shell_ret = 0
		output = ''

		sudo_cmd = 'sudo -u %s ' %(exec_user)

		run_cmd = sudo_cmd+cmd
		self._logout(run_cmd)
		try:
			p = subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in p.stdout.readlines():
				self._logout('shell stdout : '+line)
				output = output+line
			shell_ret = p.wait()
		except Exception, e:
			self._logout('shell err : '+str(e))
			output = ''
			shell_ret = NG
		self._logout('shell ret =  : '+str(shell_ret))

		return [shell_ret, output]
	
	def get_br_agent_name(self):
		return BR_AGENT

	def get_macaddr(self, device_name):

		nic_name=[]
		nic_mac_addr=[]
		self.ori.set_auth(self.auth)
		data=self.ori.get_nic(device_name)

		if -1 != data[0]:
			data1={}
			data1=data[1]
			for i in range(0,len(data1)):
				nic_name.append(data1[i]['nic_name'])
				nic_mac_addr.append(data1[i]['mac_address'])
		else:
			return [NG, 'not found nic', '']

		return [OK, nic_name, nic_mac_addr]

	def shellcmd_exec_localhost(self, cmd):
		shell_ret=0
		output=''

		run_cmd=cmd
		self._logout(run_cmd)
		try:
			p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in p.stdout.readlines():
				self._logout('shell stdout : '+line)
				output=output+line
			shell_ret=p.wait()
		except Exception, e:
			self._logout('shell err : '+str(e))
			output=''
			shell_ret= NG
		self._logout('shell ret =  : '+str(shell_ret))

		return [shell_ret, output]

