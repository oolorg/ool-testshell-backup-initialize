import sys
import pexpect
import subprocess
import os
import logging

EXPECT_PASSWD='.*password:.*'
EXPECT_YESNO='.*(yes/no)? '
SEND_YES='yes'

DEBUG_FLAG='ON'
PSLIST_FILE='pslist.txt'
PSLIST_TITLE='PSLIST:'

class pslist_backup:

	def __init__(self, local_user, bk_ip, bk_uname, bk_upw):
		self.local_user=local_user
		self.bk_ip   =bk_ip
		self.bk_uname=bk_uname
		self.bk_upw  =bk_upw
		self.logger  =logging
		
	def _sshkey_generate(self, exec_user):
		#known host key_delete
		cmd='ssh-keygen -f "/home/%s/.ssh/known_hosts" -R %s' %(exec_user, self.bk_ip)

		ret = self._shellcmd_exec(exec_user, cmd)

		#key_generate
		flg = os.path.exists("/home/%s/.ssh/id_rsa" %(exec_user))
		if not flg:
			self.logger.debug('%sPlese make /home/%s/.ssh/id_rsa IP=%s' %(PSLIST_TITLE, exec_user, self.bk_ip))
			return ret

		#ssh_copy_id
		self.logger.debug('%sssh_copy_id exec IP=%s' %(PSLIST_TITLE, self.bk_ip) )

		ret = self._ssh_copyid(exec_user, self.bk_ip, self.bk_uname, self.bk_upw)
		return ret

	def _ssh_copyid(self, exec_user, dst_ip, dst_user, dst_pw):
		ssh_newkey = 'Are you sure you want to continue connecting'

		pub_key_path =  '/home/' + exec_user + '/.ssh/id_rsa.pub'

		p=pexpect.spawn('sudo -u %s ssh-copy-id -i %s %s@%s' %(exec_user,pub_key_path, dst_user, dst_ip))
		p.timeout=120
		p.logfile=sys.stdout

		i=p.expect([ssh_newkey,'password:',pexpect.EOF])
		if i==0:
			p.sendline('yes')
			i=p.expect([ssh_newkey,'password:',pexpect.EOF])
		if i==1:
			p.sendline(dst_pw)
			p.expect(pexpect.EOF)
		elif i==2:
			pass
		print p.before # print out the result

		return 0

	def _shellcmd_exec(self, exec_user, cmd):
		shell_ret=0

		sudo_cmd='sudo -u %s ' %(exec_user)

		run_cmd=sudo_cmd+cmd
		try:
			p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in p.stdout.readlines():
				self.logger.debug('%sshell stdout :%s' %(PSLIST_TITLE, line) )
			shell_ret=p.wait()
		except Exception, e:
			self.logger.debug('%sshell err :%s' %(PSLIST_TITLE, str(e)) )
			shell_ret= 1
		self.logger.debug('%sshell ret :%s' %(PSLIST_TITLE, str(shell_ret)) )
		
		return shell_ret

	def set_logger(self, logObj):
		self.logger = logObj

	def set_pslist(self, src_file, back_dir):
		#sshkey_generate
		ret = self._sshkey_generate(self.local_user)

		if 0 != ret:
			self.logger.debug('%s#### set_pslist sshkey_generate err src=%s bk_dir=%s' %(PSLIST_TITLE, src_file, back_dir) )
			return -1

		#pslist_info copy
		cmd='scp %s %s@%s:%s/%s' %(src_file, self.bk_uname, self.bk_ip, back_dir, PSLIST_FILE)
		print cmd
		ret = self._shellcmd_exec(self.local_user, cmd)
		if 0 != ret:
			self.logger.debug('%sset_pslist copy err cmd=%s' %(PSLIST_TITLE, cmd) )
			return ret

		return 0

	def get_pslist(self, dst_file, back_dir):
		#sshkey_generate
		ret = self._sshkey_generate(self.local_user)

		if 0 != ret:
			self.logger.debug('%s#### get_pslist sshkey_generate err src=%s bk_dir=%s' %(PSLIST_TITLE, dst_file, back_dir) )
			return -1

		#pslist_info copy
		cmd='scp %s@%s:%s/%s %s' %(self.bk_uname, self.bk_ip, back_dir, PSLIST_FILE, dst_file)
		ret = self._shellcmd_exec(self.local_user, cmd)
		if 0 != ret:
			self.logger.debug('%sget_pslist copy err cmd=%s' %(PSLIST_TITLE, cmd) )
			return ret

		return 0

