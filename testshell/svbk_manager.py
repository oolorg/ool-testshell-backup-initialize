#add backup
import time
import os
import subprocess
import pexpect
import ool_rm_if
import datetime
import commands
import ConfigParser
import psbk_manager
import traceback
import json
import svbk_conflict
import svbk_utls

# Physical Switch Backup/Restore Manager
#disk size
LIMIT_DISK_SIZE_G=1
INTERVAL_TIME=10
CANCEL="off"

CONFIG_FILE = 'settings.ini'

NAME_INDEX  =0
IP_INDEX    =1
USER_INDEX  =2
PW_INDEX    =3

STORAGE_SV  =0
START_INDEX =1

R_END_STATUS="restore_ok"
R_NG_STATUS="restore_ng"

B_END_STATUS="backup_ok"
B_NG_STATUS="backup_ng"


#FILEDIR="/etc/backuprestore"
FILEDIR="/etc/ool_br_rest"
BASE_DIR_NAME="/backup"
# for TestShell
INIT_NODE=0

OK=0
NG=1

#DEBUG="ON"
DEBUG="OFF"

B_STOP_FILENAME="/b_stop"
R_STOP_FILENAME="/r_stop"
LOOP_TIME_OUT=60
RESTORE_MAX_INDEX=2


MODE_BACKUP = "backup"
MODE_RESTORE= "restore"
MODE_NONE= "none"

SW="sw "
SV="sv "

SV_NAME=0
SW_NAME=1
SV_NODE=2
SW_NODE=3
SV_P_NODE=4
SW_P_NODE=5
SV_BACKENDS=6

BACKUP_DATA_SV = "server"
BACKUP_DATA_SW = "switch"

BACKUP_DATA_KEY_NAME     ="name"
BACKUP_DATA_KEY_P_ID     ="parent_id"
BACKUP_DATA_KEY_BACKENDS ="backends"
BACKUP_DATA_KEY_N_ID ="id"

###############################
#Server Backup Up Manager
###############################
class svbk_manager:
    def __init__(self,logObj):
        self.logObj=logObj
        self.limit_disk_size=LIMIT_DISK_SIZE_G
        self.interval_time=INTERVAL_TIME
        self.loop_timeout_m=LOOP_TIME_OUT
        self.opencenter_server_name=""
        self.restore_maxFolderNum_OpenOrion = RESTORE_MAX_INDEX
        self.storage_server_name=""
        self.logpath="/var/log/br/"
        self.logfile="ddd.log"
        self.token=""
        self.date_time = datetime.datetime.today()
        self.folder_date_str = self.date_time.strftime("%Y_%m%d_%H%M%S---")
        self.log_tilte=''

    def get_node_name(self, api, node_id):
        node = api.node_get_by_id(node_id)
        return node['name']

    # for TestShell
    def set_log_Title(self, log_title="TS_"):
        self.log_tilte=log_title

    # for TestShell
    def br_log(self, node_id, name, br_mode, log):

        self.logObj.debug('%sBRLOG ID=%s NAME=%s %s:%s' %(self.log_tilte, node_id, name, br_mode, log))

        d = datetime.datetime.today()
        tm= d.strftime("%m%d %H:%M:%S")

        f = open(self.logpath+self.logfile, 'a+') 
        f.write('%s %sBRLOG ID=%s NAME=%s %s:%s \n' %(tm, self.log_tilte, node_id, name, br_mode, log))
        f.close()

        return 0

    # for TestShell
    def b_log(self, node_id, name, log):

        self.logObj.debug('%sBRLOG ID=%s NAME=%s b: %s' %(self.log_tilte, node_id, name, log))

        d = datetime.datetime.today()
        tm= d.strftime("%m%d %H:%M:%S")

        f = open(self.logpath+self.logfile, 'a+') 
        f.write('%s %sBRLOG ID=%s NAME=%s b: %s' %(tm, self.log_tilte, node_id, name, log))
        f.close()

        return 0

    def _ssh_copyid(self, exec_user,DEST_IP, DEST_USER, DEST_PW):
        ssh_newkey = 'Are you sure you want to continue connecting'

        pub_key_path =  '/home/' + exec_user + '/.ssh/id_rsa.pub'

        p=pexpect.spawn('sudo -u %s ssh-copy-id -i %s %s@%s' %(exec_user,pub_key_path,DEST_USER,DEST_IP))
        p.timeout=120

        i=p.expect([ssh_newkey,'password:',pexpect.EOF])
        if i==0:
            p.sendline('yes')
            i=p.expect([ssh_newkey,'password:',pexpect.EOF])
        if i==1:
            p.sendline(DEST_PW)
            p.expect(pexpect.EOF)
        elif i==2:
            pass
        print p.before # print out the result

        return 0

    def shellcmd_exec(self, exec_user, br_mode, node_id, name, cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        #self.logger.debug(run_cmd)
        self.br_log(node_id, name, br_mode, run_cmd)
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('shell '+line)
                self.br_log(node_id, name, br_mode, 'shell stdout : '+line)
            shell_ret=p.wait()
        except Exception, e:
            #self.logger.debug(e)
            self.br_log(node_id, name, br_mode, 'shell err : '+str(e))
            shell_ret= 1
        #self.logger.debug(shell_ret)
        self.br_log(node_id, name, br_mode, 'shell ret =  : '+str(shell_ret))

        return shell_ret

    def shellcmd_exec_br_state(self, exec_user, br_mode, node_id, name, cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('status = '+line)
                self.br_log(node_id, name, br_mode, 'status = '+line)
            shell_ret=p.wait()
        except Exception, e:
            self.br_log(node_id, name, br_mode, 'shell br_state err : '+e)
            shell_ret= 1
        return shell_ret

    def shellcmd_exec_rest_diskSize(self, exec_user, br_mode, node_id, name,cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('status = '+line)
                self.br_log(node_id, name, br_mode, 'Avaliavle Disk Size(K):'+line)
                ret_val=line
            shell_ret=p.wait()
        except Exception, e:
            self.br_log(node_id, name, br_mode, 'shell br_state err : '+e)
            shell_ret= 1

        return [shell_ret, int(ret_val)]

    def make_exec(self, exec_user, DEST_IP, DEST_USER, DEST_PW, FILEDIR, br_mode, node_id, name):

        #known host key_delete
        cmd='ssh-keygen -f "/home/%s/.ssh/known_hosts" -R %s' %(exec_user, DEST_IP)
        #cmd='sudo -u %s ssh-keygen -f "/home/%s/.ssh/known_hosts" -R %s' %(DEST_USER, DEST_USER, DEST_IP)

        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        #if ret!=0:
            #self.logger.debug('err')
            #return ret

        #key_generate
        flug = os.path.exists("/home/%s/.ssh/id_rsa" %(exec_user))
        if not flug:
            #self.logger.debug('Plese make /home/%s/.ssh/id_rsa' %(exec_user))
            self.br_log(node_id, name, br_mode, 'Plese make /home/%s/.ssh/id_rsa IP=%s' %(exec_user, DEST_IP) )

            return ret

            #self.logger.debug('DDD rsa Key Gen')
            #cmd="""sudo -u %s echo -e "y\n" | ssh-keygen -q -N "" -t rsa -f /home/%s/.ssh/id_rsa""" %(DEST_USER,DEST_USER)
            #ret = self.shellcmd_exec(cmd)
            #if ret!=0:
            #    self.logger.debug('err')
            #    return ret


        #ssh_copy_id
        #self.logger.debug('ssh_copy_id exec')
        self.br_log(node_id, name, br_mode, 'ssh_copy_id exec IP=%s' %(DEST_IP) )

        self._ssh_copyid(exec_user,DEST_IP, DEST_USER, DEST_PW)

        #key_root_copy
        cmd='scp %s/key_copy.py %s@%s:.' %(FILEDIR, DEST_USER, DEST_IP)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            #self.logger.debug('err')
            self.br_log(node_id, name, br_mode, 'key_root_copy err IP=%s' %(DEST_IP))
            return ret

        #pexpect copy
        cmd='scp %s/pexpect.py %s@%s:.' %(FILEDIR, DEST_USER, DEST_IP)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            #self.logger.debug('err')
            self.br_log(node_id, name, br_mode, 'pexpect copy err IP=%s' %(DEST_IP))
            return ret

        #key_copy.py exec
        cmd='ssh %s@%s python ./key_copy.py  %s %s' %(DEST_USER, DEST_IP,DEST_USER, DEST_PW)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            #self.logger.debug('err')
            self.br_log(node_id, name, br_mode, 'key_copy.py exec err IP=%s' %(DEST_IP))
            return ret

        return 0


    def _parent_list(self, api, starting_node):  # pragma: no cover
        ret = list()
        cnt = 0
        node = starting_node
        while 'parent_id' in node['facts']:
            cnt = cnt + 1
            ret.append(node['facts']['parent_id'])
            node = api.node_get_by_id(node['facts']['parent_id'])
            if isinstance(node,type(None)):
                self.br_log(0, '_parent_list', 'b', 'node is none:cnt=%s' %(cnt))
                self.br_log(0, '_parent_list', 'b', 'parent=%s' %(ret))
                return ret
        return ret

    def br_precheck(self, api, node_id, br_mode, cluster_name):
        #########
        #run chek
        ######### 
        # for TestShell
        # set conflict info
        ret_info = self.get_node_info(api, node_id, cluster_name, br_mode)
        server_node_name = ret_info[SV_NAME]
        switch_node_name = ret_info[SW_NAME]
        node_list=','.join(server_node_name)
        node_list='%s,%s' % (node_list, ','.join(switch_node_name))
        conflict=svbk_conflict.svbk_conflict()
        conflict.set_loger(self.logObj)
        ret = conflict.chk_mode_state(br_mode, cluster_name, node_list)

        return ret

    def set_mode_state(self, api, node_id, state):

        # for TestShell
        # set conflict info
        group_name=self.get_node_name(api, node_id)
        conflict=svbk_conflict.svbk_conflict()
        conflict.set_loger(self.logObj)
        conflict.set_mode_state(group_name, state)

    def set_parent_id(self, clster_name, node_id, br_mode, api, name, p_id):

        nodeinfo = api._model_get_first_by_query(
            'nodes',
            'name="%s"' % name)

        if isinstance(nodeinfo,type(None)):
            self.br_log(node_id, clster_name, br_mode, 'node is none:name=%s err' %(name))
            return

        #p_id = 2

        get_node_id = int(nodeinfo['id'])
        self.br_log(node_id, clster_name, br_mode, 'get_node_id=%s set parent_id=%s' %(get_node_id,p_id))

        api._model_create('facts', {'node_id': get_node_id,
                      'key': 'parent_id',
                      'value': p_id})

        nodes = api._model_query(
            'nodes',
            'name="%s"' % name)

        #if nodes is any
        if len(nodes) != 1:
            self.br_log(node_id, clster_name, br_mode, 'len(nodes)=%s err ' %(len(nodes)))

        return

    def get_node_info(self, api, node_id, name, br_mode):
        #get all node id
        server_node_list   = []
        server_node_name   = []
        server_parent_node = []
        server_backends    = []


        switch_node_list   = []
        switch_node_name   = []
        switch_parent_node = []

        nodes = api._model_get_all('nodes')

        for node in nodes:
            #node info get
            nodeinfo = api.node_get_by_id(node['id'])
            parent_list = []

            #node parent list get
            parent_list = self._parent_list(api, nodeinfo)

            #is novacluster_id in parent_list
            if node_id in parent_list:
                #is novacluster_id in parent_list
                if 'backends' in nodeinfo['facts']:
                    #is agent in facts.backend
                    if 'agent' in nodeinfo['facts']['backends']:
                        if 'sdn' in nodeinfo['facts']['backends']:
                            switch_node_list.append(node['id'])
                            switch_node_name.append(node['name'])
                            switch_parent_node.append(node['facts']['parent_id'])

                        else:
                            server_node_list.append(node['id'])
                            server_node_name.append(node['name'])
                            server_parent_node.append(node['facts']['parent_id'])
                            server_backends.append(node['facts']['backends'])



        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SERVER_NODE_LIST  : %s' % server_node_list)
        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SERVER_PARENT_LIST: %s' % server_parent_node)
        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SERVER_BACKENDS_LIST: %s' % server_backends)

        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SWITCH_NODE_LIST  : %s' % switch_node_list)
        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SWITCH_NODE_NAME  : %s' % switch_node_name)
        self.br_log(node_id, name, br_mode, '*** OpenCenter DB SWITCH_PARENT_LIST: %s' % switch_parent_node)

        #self.logger.debug('*** SERVER_NODE_LIST: %s' % server_node_list)
        #self.logger.debug('*** SERVER_NODE_NAME: %s' % server_node_name)

        retdata =[server_node_name, switch_node_name, server_node_list, switch_node_list, 
                    server_parent_node, switch_parent_node,server_backends]

        return retdata

    def get_node_info_from_serverFile(self, api, node_id, name, br_mode, node_info):

        #get all node id
        server_node_list   = []
        server_node_name   = []
        server_parent_node = []
        server_backends    = []


        switch_node_list   = []
        switch_node_name   = []
        switch_parent_node = []

        for i in range(len(node_info[BACKUP_DATA_SV])):
            server_node_list.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_N_ID])
            server_node_name.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_NAME])
            server_parent_node.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_P_ID])
            server_backends.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_BACKENDS])

        for i in range(len(node_info[BACKUP_DATA_SW])):
            switch_node_list.append(node_info[BACKUP_DATA_SW][i][BACKUP_DATA_KEY_N_ID])
            switch_node_name.append(node_info[BACKUP_DATA_SW][i][BACKUP_DATA_KEY_NAME])
            switch_parent_node.append(node_info[BACKUP_DATA_SW][i][BACKUP_DATA_KEY_P_ID])


        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_NODE_LIST  : %s' % server_node_list)
        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_PARENT_LIST: %s' % server_parent_node)
        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_BACKENDS_LIST: %s' % server_backends)


        self.br_log(node_id, name, br_mode, '*** storege server file SWITCH_NODE_LIST  : %s' % switch_node_list)
        self.br_log(node_id, name, br_mode, '*** storege server file SWITCH_NODE_NAME  : %s' % switch_node_name)
        self.br_log(node_id, name, br_mode, '*** storege server file SWITCH_PARENT_LIST: %s' % switch_parent_node)

        retdata =[server_node_name, switch_node_name, server_node_list, switch_node_list, 
                    server_parent_node, switch_parent_node,server_backends]

        return retdata

    def get_user_name(self, host_name):
        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        data = ori.get_device(host_name)

        if -1 != data[0]:
            data1={}
            data1=data[1]
            return [0, data1['user_name']]
        else:
            return [-1,'NG']

    def set_server_info(self, node_id, server_info_list,server_name, name, br_mode):
        return self.set_server_info_Common(node_id, server_info_list,server_name, name, br_mode, 'M-Plane')

    def set_server_info_CPlane(self, node_id, server_info_list,server_name, name, br_mode):
        return self.set_server_info_Common(node_id, server_info_list,server_name, name, br_mode, 'C-Plane')

    def set_server_info_SPlane(self, node_id, server_info_list,server_name, name, br_mode):
        return self.set_server_info_Common(node_id, server_info_list,server_name, name, br_mode, 'S-Plane')

    def set_server_info_Common(self, node_id, server_info_list,server_name, name, br_mode, Plane):

        #make Resource Manager Instance
        temp_server_info=server_info_list

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        self.br_log(node_id, name, br_mode, "ddddd self.token:%s" %(self.token))

        #get IP address
        data=ori.get_nic_traffic_info(server_name, Plane)
        #get ip address
        if -1 != data[0]:
            data1={}
            data1=data[1][0]
            temp_server_info[1]=data1['ip_address']

            #print data1['nic_name']
            #print data1['ip_address']
        else:
            #print "<nic traffic_type error:" + data[1] +">"
            self.br_log(node_id, name, br_mode, "nic traffic_type error:%s" %(data[1]))
            ret = 1 
            return [ret, temp_server_info]

        #get username password
        data=ori.get_device(server_name)
        ret=0

        if -1 != data[0]:
            data1={}
            data1=data[1]

            #input server info
            temp_server_info[0]=server_name
            #temp_server_info[1]=data1['ip_address']
            temp_server_info[2]=data1['user_name']
            temp_server_info[3]=data1['password']

            #check info (length)
            for i in range(0, 4):
                self.br_log(node_id, name, br_mode, "set_server_info temp_server_info[%s]=%s" %(i,temp_server_info[i]))
                #self.logger.debug("info i=%s info=%s" %(i,temp_server_info[i]))
                char_len=len(temp_server_info[i])
                if char_len == 0:
                    self.br_log(node_id, name, br_mode, "set_server_info len=0 err ")
                    ret=1
                    break
        else:
            self.br_log(node_id, name, br_mode, "set_server_info <device error:+ data[0]=%s " %(data[0]))
            #self.logger.debug("<device error:"+ data[1] +">")
            ret=1

        return [ret, temp_server_info]

    def make_serverinfo_file(self, server_info, node_id):
        ret=0
        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")

        TMP_FILE='/tmp/server_info_' + tm + "node_id" +str( node_id )
        f=open(TMP_FILE, 'w')
        f.write(server_info)
        f.close()

        return [ret, TMP_FILE]

    def get_restore_foldername(self, **kwargs):

        #tmp_str = kwargs['restore_name']

        #end=tmp_str.rindex(' ')
        #restore_folder_name = tmp_str[end+1:len(tmp_str)]

        restore_folder_name = kwargs['restore_name']

        return restore_folder_name

    def make_log_file_name(self, clster_name, node_id, br_mode, **kwargs):
        ret=0
        d = self.date_time
        tm1= d.strftime("%m%d_%H%M_%S_")

        self.logfile='%s%s%s_%s_ID%s_N%s' % (self.log_tilte, tm1, br_mode, clster_name, node_id, self.folder_date_str)

        if "b" == br_mode :
            self.logfile=self.logfile + '%s.log' % (kwargs['backup_name'])
        else:
            self.logfile=self.logfile + '%s.log' % (kwargs['restore_name'])

        return ret

    def set_system_param(self,clster_name,node_id,br_mode):

        conf = ConfigParser.SafeConfigParser()

        set_file_path=FILEDIR+"/"+CONFIG_FILE

        ret=conf.read(set_file_path)

        if len(ret)==0 :
            self.br_log(node_id, clster_name, br_mode, '####set_system_param ng file is nothing ')
            return NG

        self.br_log(node_id, clster_name, br_mode, '####set_system_param file_name :%s' %(ret[0]) )

        #if ret[0]!=CONFIG_FILE :
        #    self.br_log(node_id, clster_name, br_mode, '####set_system_param ng 2')
        #    return NG


        self.limit_disk_size = int(conf.get('options', 'limit_disk_size'))
        self.interval_time = int(conf.get('options', 'interval_time'))
        self.opencenter_server_name=conf.get('options','opencenter_server_name')
        self.storage_server_name = conf.get('options', 'storage_server_name')
        self.restore_maxFolderNum_OpenOrion = int(conf.get('options', 'restore_maxFolderNum_OpenOrion'))
        self.loop_timeout_m = int(conf.get('options', 'loop_timeout_m'))
        #self.rm_pw = conf.get('options', 'rm_pw')

        self.br_log(node_id, clster_name, br_mode, '####read_file limit_disk_size :%s' %(self.limit_disk_size) )
        self.br_log(node_id, clster_name, br_mode, '####read_file interval_time :%s' %(self.interval_time) )
        self.br_log(node_id, clster_name, br_mode, '####read_file opencenter_server_name :%s' %(self.opencenter_server_name) )
        self.br_log(node_id, clster_name, br_mode, '####read_file storage_server_name :%s' %(self.storage_server_name) )
        self.br_log(node_id, clster_name, br_mode, '####read_file restore_maxFolderNum_OpenOrion :%s' %(self.restore_maxFolderNum_OpenOrion) )
        self.br_log(node_id, clster_name, br_mode, '####read_file loop_timeout_m :%s' %(self.loop_timeout_m) )
        #self.br_log(node_id, clster_name, br_mode, '####read_file rm_pw :%s' %(self.rm_pw) )

        return OK

    def make_nodeinfo_file(self, clster_name, node_id, br_mode, backup_node_data_info):

        #define node list
        server_node_name   = backup_node_data_info[SV_NAME]
        switch_node_name   = backup_node_data_info[SW_NAME]
        server_p_node_list = backup_node_data_info[SV_P_NODE]
        switch_p_node_list = backup_node_data_info[SW_P_NODE]
        server_backends    = backup_node_data_info[SV_BACKENDS]
        server_node_id     = backup_node_data_info[SV_NODE]
        switch_node_id     = backup_node_data_info[SW_NODE]

        #check server
        if len(server_node_name) != len(server_p_node_list):
            self.br_log(node_id, clster_name, br_mode, 
                '#### server data length check Err  server_node_name=%s server_p_node_list=%s' %(server_node_name,server_p_node_list))
            return [1, "null"]

        server_num = len(server_node_name)

        #check switch
        if len(switch_node_name) != len(switch_p_node_list):
            self.br_log(node_id, clster_name, br_mode, 
                '#### server data length check Err  server_node_name=%s server_p_node_list=%s' %(switch_node_name,switch_p_node_list))
            return [1, "null"]

        switch_num = len(switch_node_name)

        jdata ={"server": [],"switch": []}

        #set server node
        for i in range(server_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(server_node_name[i]),
                  BACKUP_DATA_KEY_P_ID: str(server_p_node_list[i]),
                  BACKUP_DATA_KEY_N_ID: str(server_node_id[i]),
                  BACKUP_DATA_KEY_BACKENDS: str(server_backends[i])}
            jdata[BACKUP_DATA_SV].append(tmp)

        #set switch node
        for i in range(switch_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(switch_node_name[i]),
                  BACKUP_DATA_KEY_N_ID: str(switch_node_id[i]),
                  BACKUP_DATA_KEY_P_ID: str(switch_p_node_list[i])}
            jdata[BACKUP_DATA_SW].append(tmp)

        #file name make
        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")
        TMP_FILE='/tmp/node_info' + tm

        #file OutPut
        with open(TMP_FILE, 'w') as f:
            json.dump(jdata, f, sort_keys=True, indent=4)

        return [0, TMP_FILE]

    def make_dbset_nodeinfo(self, clster_name, node_id, br_mode, node_list, kind):
        
        #define return node list
        ret_node_list=[]

        #set kind all node
        for node in node_list:
            tmp = kind + node
            ret_node_list.append(tmp)

        return ret_node_list

    def setdb_backup_data(self,clster_name,node_id,br_mode, folder_name, server_list, switch_list):

        #make regist info
        #clustername_ID8
        db_clster_name=  clster_name + "_ID%s" %(node_id)

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        #make server switch info
        #db_server_list = self.make_dbset_nodeinfo(clster_name, node_id, br_mode, server_list, SV)
        #db_switch_list = self.make_dbset_nodeinfo(clster_name, node_id, br_mode, switch_list, SW)

        db_server_list = server_list
        db_switch_list = switch_list

        #server & switch 
        db_node_list = db_server_list + db_switch_list

        #folder_name
        #d = datetime.datetime.today()
        #db_folder_name= "t_" + str(d.strftime("%Y-%m-%d %H:%M:%S  "))  + folder_name
        db_folder_name= self.folder_date_str  + folder_name

        #db_folder_name= "test1_"  + folder_name

        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_clster_name = %s' %(db_clster_name))
        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_folder_name = %s' %(db_folder_name))
        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_node_list = %s'   %(db_node_list))

        #set db
        data=ori.set_backup_data(db_clster_name, db_folder_name, db_node_list)

        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode, '#### set_backup_data err  data=%s' %(data))
            return 1

        return 0

    def getdb_backup_alldata(self, clster_name, node_id, br_mode, api):

        #get cluster name
        node = api.node_get_by_id(node_id)
        clster_name = node['name']

        #make regist db cluster name 
        db_clster_name=  str(clster_name + "_ID%s" %(node_id))

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        data=ori.get_backup_cluster(db_clster_name)

        backupAllData=[]

        #db err
        if 0 != data[0]:
            self.br_log(node_id, clster_name, br_mode, '#### getdb_backup_alldata err')

            return [1, backupAllData]

        #get registed db alldata
        backupAllData = data[1]

        return [0, backupAllData]

    def getdb_backup_allnode(self, clster_name, node_id, br_mode, api, backup_name_key):

        data = self.getdb_backup_alldata(clster_name, node_id, br_mode, api)

        allnode = []

        if 0 != data[0]:
            self.br_log(node_id, clster_name, br_mode, '#### getdb_backup_allnode err')
            return [1, allnode]

        #self.br_log(node_id, clster_name, br_mode, 'getdb_backup_alldata: data[1] %s'%(data[1]))

        backupAllData  = data[1]

        for i in range(len(backupAllData)):
            if backupAllData[i]["backup_name"] == backup_name_key:
                allnode = backupAllData[i]["device"]


        self.br_log(node_id, clster_name, br_mode, '#### getdb_backup_allnode allnode:%s' %(allnode))

        return [0, allnode]

    def set_parent_id_all(self, clster_name, node_id, br_mode, api, node_name_list, parent_id_list):

        self.br_log(node_id, clster_name, br_mode, '#### set_parent_id_all')

        for i in range(len(node_name_list)):
            self.set_parent_id(clster_name, node_id, br_mode, api, node_name_list[i] , int(parent_id_list[i]))

        return

    def check_novacluster_node(self,clster_name,node_id,br_mode, backupedCluster_node,api):

        self.br_log(node_id, clster_name, br_mode, '#### R NovaCluster_CheckStart')

        #data=self.getdb_backup_allnode( clster_name, node_id, br_mode, api, backup_name_key)
        #if 0 != data[0]:
        #    self.br_log(node_id, clster_name, br_mode, '####R NovaClaster_CheckStart getdb_backup_allnode err')
        #    return 1

        #get node opencenter db
        ret_info = self.get_node_info(api, node_id, clster_name, br_mode)
        server_node_name = ret_info[SV_NAME]
        switch_node_name = ret_info[SW_NAME]

        nowClaster_node     = server_node_name + switch_node_name

        self.br_log(node_id, clster_name, br_mode, '#### R server node check  backupedCluster_node:%s' %(backupedCluster_node ))
        self.br_log(node_id, clster_name, br_mode, '#### R server node check  nowClaster_node      %s' %(nowClaster_node ))

        set_ab=set(nowClaster_node)-set(backupedCluster_node)
        if 0 != len(set_ab):
            self.br_log(node_id, clster_name, br_mode, '#### R server node check Err unnecessary node:%s' %(set_ab))
            return 1

        self.br_log(node_id, clster_name, br_mode, '#### R NovaClaster_Cherck OK')

        return 0

    def set_token(self,clster_name,node_id,br_mode, **kwargs):

        cookie_data = kwargs['auth']
        token_serch_char="iPlanetDirectoryPro="

        cookie_split=cookie_data.split(";")

        token=""

        for data in cookie_split:
            search_len = data.find(token_serch_char)
            if search_len != -1:
                token = data[search_len + len(token_serch_char):]
                break

        if len(token) == 0:
            self.br_log(node_id, clster_name, br_mode, '###token is none key only:%s' %( cookie_data ))
            return 1

        self.set_token_value(clster_name,node_id,br_mode,token)

        return 0

    def set_token_value(self, clster_name='', node_id=INIT_NODE, br_mode='r', token='????'):
        self.token = token
        self.br_log(node_id, clster_name, br_mode, '###token is aa: %s' % self.token)

        return 0

    def trans_node_info(self,node_id, CLSTER_NAME, br_mode, backup_node_data_info, server_info, EXEC_USER, NODE_LIST_FILE):

        ##########################
        #set  backup info to storage
        ##########################

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### make_nodeinfo_file  ')
        retdata = self.make_nodeinfo_file(CLSTER_NAME, node_id, br_mode, backup_node_data_info)
        if 0 != retdata[0]:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### make_nodeinfo_file ng ')
            return 1

        file_path = retdata[1]

        #scp to storage :backup info file
        cmd="scp %s root@%s:%s " %(file_path, server_info[STORAGE_SV][IP_INDEX] , NODE_LIST_FILE)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### B backup info trans to strage err')
            return 1

        #remove file
        cmd="rm %s " %(file_path)
        commands.getoutput(cmd)

        return 0

    def get_restoredb_list(self,node_id, clster_name, br_mode, db_clster_name):

        #self.br_log(node_id, clster_name, br_mode, "call ori.get_backup_cluster start db_clster_name=%s" %(db_clster_name))

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        data=ori.get_backup_cluster(db_clster_name)

        backup_list=[]

        #db err
        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode, " ori.get_backup_cluster err data=%s  db_clster_name=%s" %(data,db_clster_name))
            return [1, ""]

        #get registed db list
        backupAllData = data[1]

        for i in range(len(backupAllData)):
            backup_list.append(backupAllData[i]["backup_name"])


        return [0, backup_list]

    def get_restore_folder_list(self,node_id, clster_name, br_mode, db_clster_name):

        #get db list
        retdata = self.get_restoredb_list(node_id, clster_name, br_mode,db_clster_name)
        if 0 !=retdata[0]:
            self.br_log(node_id, clster_name, br_mode, " get_restoredb_list err")
            return [1, retdata[1]]

        restore_list = retdata[1]

        #sort new data is top
        restore_list.sort(reverse=True)

        if len(restore_list) == 0:
            self.br_log(node_id, clster_name, br_mode, "restore_list is 0")
            return [1, ""]

        return [0, restore_list]

    def delete_restoredb_list(self,node_id, CLSTER_NAME, br_mode, db_clster_name, dbfolder):

        #make instance
        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        #del database
        data=ori.del_backup(db_clster_name, dbfolder)

        #db err
        if -1 == data[0]:
            self.br_log(node_id, CLSTER_NAME, br_mode, "###Delete ori.del_backup  err data=%s  db_clster_name=%s dbfolder=%s" %(data, db_clster_name, dbfolder))
            return 1

        return 0

    def delete_data_and_dblist_opencenter(self,node_id, CLSTER_NAME, br_mode, server_info, SAVE_DIR_NAME, EXEC_USER):

        self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete Backup Data start')

        #################
        #get retore folder name
        #################
        db_clster_name=  CLSTER_NAME + "_ID%s" %(node_id)

        getArray = self.get_restore_folder_list(node_id, CLSTER_NAME, br_mode, db_clster_name)

        #folder serch :ng check
        if getArray[0] != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder get err')
            return 1

        backupFolderList = getArray[1]

        self.br_log(node_id, CLSTER_NAME, br_mode, '###backupFolderList=%s' %(backupFolderList))

        #################
        #folder num check
        #################
        if( len(backupFolderList) <= self.restore_maxFolderNum_OpenOrion  ):
            msg='###folder num is OK  len(backupFolderList)=%s restore_maxFolderNum_OpenOrion=%s ' %(len(backupFolderList), self.restore_maxFolderNum_OpenOrion)
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return 0

        ###################
        #delete DB & file
        ###################
        for i in range(self.restore_maxFolderNum_OpenOrion  , len(backupFolderList) ):

            rmdir=BASE_DIR_NAME+"/"+ db_clster_name+"/"+backupFolderList[i]

            self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete db data   backupFolderList[%s]=%s' %(i,backupFolderList[i]))
            self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete dir[%s]=%s' %(i,rmdir))

            ###########
            #db delete#
            ###########
            ret = 0
            ret = self.delete_restoredb_list(node_id, CLSTER_NAME, br_mode,  db_clster_name, backupFolderList[i] )
            if ret != 0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete db data  backupFolderList[%s]=%s' %(i,backupFolderList[i]))
                continue

            #######################
            #db folder data delete ddd
            #######################
            #cmd='ssh root@%s rm -rf %s  2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX], rmdir )
            cmd='ssh root@%s rm -rf %s  > /dev/null' %(server_info[STORAGE_SV][IP_INDEX], rmdir )
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### Dell Restore Status File Err ')
                continue

        return 0

    def backup_cluster(self, api, node_id, **kwargs):
        try:
            br_mode="b"
            CLSTER_NAME = self.get_node_name(api, node_id)

            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            var = kwargs['backup_name']
            if not var:
                self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder name is none')
                return [NG, '###backup folder name is none']

            self.make_log_file_name(CLSTER_NAME,node_id,br_mode,**kwargs)

            ###################
            #check mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')
            ret = self.br_precheck(api, node_id, br_mode, CLSTER_NAME)
            if ret==1:
                return [OK, "ok"]
            elif ret==-1:
                msg='#### restore runnning then err'
                return [NG, msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')

            ###############
            ####Backup ####
            ###############
            retArray = self.backup_cluster_sub(api, node_id, **kwargs)

            #set mode none
            self.set_mode_state(api, node_id, MODE_NONE)

            return retArray

        except Exception,e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()) )

            #set mode none
            self.set_mode_state(api, node_id, MODE_NONE)

            raise


    #####################
    #Backup Module
    #####################
    def backup_cluster_sub(self, api, node_id, **kwargs):

        #####################
        #B predefine 
        #####################

        br_mode="b"
        CLSTER_NAME = self.get_node_name(api, node_id)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Backup Start ')

        if not 'backup_name' in kwargs:
            return [NG, 'backup folder name is required']

        BACKUP_FOLDER_RNAME=CLSTER_NAME + "_ID%s" %(node_id)  +"/" +self.folder_date_str + kwargs['backup_name'] 

        self.b_log(node_id, CLSTER_NAME, 'backup_folder_name: %s' % BACKUP_FOLDER_RNAME)

        ret=self.set_system_param(CLSTER_NAME,node_id,br_mode)
        if ret!=0:
            #self.logger.debug('backup exec start err all=%d index=%d' %(server_cnt, i) )
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
            msg='#### set_system_param err'
            return [NG, msg]

        #self.b_log(node_id, CLSTER_NAME, 'DDDDDDDD: limit_disk_size=%s, ret:%s' %(self.limit_disk_size, ret))
        #return [OK, 'ok']

        #####################
        #B define
        #####################
        SAVE_DIR_NAME=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/server"
        SAVE_DIR_NAME_SWITCH=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/switch"

        #SERVER_INFO_FILE_NAME="serv_info"
        #SERVER_LIST_FILE=SAVE_DIR_NAME+"/"+SERVER_INFO_FILE_NAME

        NODE_INFO_FILE_NAME="node_info"
        NODE_LIST_FILE=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, **kwargs)

        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return [NG, '###get token ng']

        #start time get
        time1 = time.time()

        ########################
        #B Get server name for opencenterDB
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for opencenterDB')

        ret_info = self.get_node_info(api, node_id, CLSTER_NAME, br_mode)
        server_node_name = ret_info[SV_NAME]
        server_num=len(server_node_name)

        switch_node_name = ret_info[SW_NAME]
        switch_num=len(switch_node_name)
        switch_node_name_char=','.join(switch_node_name)
        #server_node_name_char=','.join(server_node_name)

        #server,switch node info
        backup_node_data_info = ret_info;

        self.br_log(node_id, CLSTER_NAME, br_mode, '*** SERVER_NODE_LIST :List %s, cnt=%s' %(server_node_name,server_num))
        self.br_log(node_id, CLSTER_NAME, br_mode, '*** SWITCH_NODE_LIST :List %s, cnt=%s' %(switch_node_name,switch_num))

        if (0 == server_num) and ( 0 == switch_num):
            #self.logger.debug('sever num = 0 then not backup')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
            return [OK, 'ok']

        ######################
        #B Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        server_cnt=server_num+1
        server_info_num=4
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ########################
        #B Set Storage Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        #retdata = self.set_server_info(node_id, server_info[0],STORAGE_SERVER_NAME, CLSTER_NAME, br_mode)
        retdata = self.set_server_info(node_id, server_info[0],self.storage_server_name, CLSTER_NAME, br_mode)
        if 0 != retdata[0]:
            #self.logger.debug('server info set err')
            self.b_log(node_id, CLSTER_NAME, '#### server info set err')

            msg='server info set err'
            return [NG, msg]

        ########################
        #B Set NovaClaster Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server info')

        for i in range(1, server_cnt):
            #self.logger.debug('ddd i=%s server_node_name=%s' %(i, server_node_name[i-1]))
            self.b_log(node_id, CLSTER_NAME, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )

            retdata = self.set_server_info(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
            if 0 != retdata[0]:
                #self.logger.debug('Set Server info set server_node_name=%s' %(server_node_name[i-1]))
                self.b_log(node_id, CLSTER_NAME, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )
                msg='Set Server info set server_node_name=%s' %(server_node_name[i-1])
                return [NG, msg]

        ################
        #B Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
#        EXEC_USER=server_info[STORAGE_SV][USER_INDEX]
        ret = self.get_user_name(self.opencenter_server_name)
        if 0 == ret[0]:
            EXEC_USER=ret[1]
        else:
            self.b_log(node_id, CLSTER_NAME, '#### Set Exec_User error' )
            msg='Set Exec_User error'
            return [NG, msg]

        #####################################
        #B Set NovaClaster Server Make Exec Env
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server Make Exec Env')

        for i in range(server_cnt):
            #self.logger.debug('backup exec start all=%d index=%d' %(server_cnt, i) )
            self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start all=%d index=%d' %(server_cnt, i))
            ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
            if ret!=0:
                #self.logger.debug('backup exec start err all=%d index=%d' %(server_cnt, i) )
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err all=%d index=%d' %(server_cnt, i) )
                msg='make_exec err'
                return [NG, msg]

        #######################
        #B Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret==0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory Err (already same name dirctory)')
            msg='Check Directory (already same name dirctory)'
            return [NG, msg]

        #################################
        #B Check Strorage Server Disk Size
        ################################
#        cmd="ssh root@%s df -k | grep '/dev/sda1' | awk '{ print $4 }' " %(server_info[STORAGE_SV][IP_INDEX])
        cmd="ssh root@%s df -k | grep '/dev/mapper/storage--vg-root' | awk '{ print $4 }' " %(server_info[STORAGE_SV][IP_INDEX])
        ret_list = self.shellcmd_exec_rest_diskSize(EXEC_USER, br_mode, node_id, CLSTER_NAME,cmd)
        if 0 != ret_list[0]:
            #self.logger.debug('Set Server info set server_node_name=%s' %(server_node_name[i-1]))
            self.b_log(node_id, CLSTER_NAME, '#### Check Strorage Server Disk Size Err =%s' %( ret_list[0] ) )
            msg='#### Check Strorage Server Disk Size Err =%s' %( ret_list[0] )
            return [NG, msg]

        #self.br_log(node_id, CLSTER_NAME, br_mode, 'ret_list[0]=%s ret_list[1]=%s' %(ret_list[0],ret_list[1]) )

        diskSize_G = ret_list[1]/(1024*1024)

        #chek disk size
        if self.limit_disk_size >= diskSize_G:
            self.b_log(node_id, CLSTER_NAME, '#### Check Strorage Server Disk Size Shortage Err limit_disk_size=%s > diskSize_G=%s ' %( self.limit_disk_size, diskSize_G ) )

            msg='#### Check Strorage Server Disk Size Shortage  Err limit_disk_size=%s > diskSize_G=%s ' %( self.limit_disk_size, diskSize_G )
            return [NG, msg]

        self.b_log(node_id, CLSTER_NAME, '#### Check Strorage SV Disk Size LIMIT(G)=%s diskSize(G)=%s ' %( self.limit_disk_size, diskSize_G ) )

        #########################################
        #B Make Backup directory to Storage Server
        #########################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Backup directory to Storage Server')

        #./dirmake "$BASE_DIR_NAME" "$CLUSTER_NAME" ${STORAGE_SV[0]} ${STORAGE_SV[1]}
        cmd='%s/dirmake  %s  %s  %s  %s' %(FILEDIR, BASE_DIR_NAME, BACKUP_FOLDER_RNAME, server_info[STORAGE_SV][IP_INDEX], server_info[STORAGE_SV][USER_INDEX])
        #ret = self.shellcmd_exec(EXEC_USER,cmd)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            #self.logger.debug('make dir err '  )
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### make dir err ')
            msg='make dir err'
            return [NG, msg]

        #######################
        #B Make Directory Check 
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Directory Check ')

        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Directory Check Err (directory is none)')
            msg='Make Directory Check Err (directory is none) '
            return [NG, msg]

        ##########################
        #set backup info to storage
        ##########################
        ret = self.trans_node_info(node_id, CLSTER_NAME, br_mode, backup_node_data_info, server_info, EXEC_USER, NODE_LIST_FILE)
        if 0 != ret:
            msg='#### set backup info to storage  ng '
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return [NG, msg]

        #DDD debug DDD
        #switch_num=0

        #DDD
        if DEBUG == "ON":
            switch_num=0
        #################
        #B switch backup
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup Start')

            #psbk=psbk_manager.psbk_manager(STORAGE_SERVER_NAME, SAVE_DIR_NAME_SWITCH)
            #psbk=psbk_manager.psbk_manager(self.storage_server_name, SAVE_DIR_NAME_SWITCH)
            psbk=psbk_manager.psbk_manager(EXEC_USER, self.storage_server_name, SAVE_DIR_NAME_SWITCH,self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.set_PS_list')

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_name_char))


            ret=psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.set_PS_list Err')
                #return self._fail(msg='####SWITCH  psbk.set_PS_list Err psbk.set_PS_list Err)')
                msg='####SWITCH  psbk.set_PS_list Err psbk.set_PS_list Err)'
                return [NG, msg]

            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_backup()')
            ret=psbk.exec_backup()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_backup() Err')
                #return self._fail(msg='####SWITCH  psbk.exec_backup() ')
                msg='####SWITCH  psbk.exec_backup() Err'
                return [NG, msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup End')

        #DDD
        if DEBUG == "ON":
            server_num=0

        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            #return self._ok()
            return [OK, 'ok']

        #########################################################################
        ret = self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server Back UP Start')

        ################
        #B Run backup
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run backup2')

        for i in range(START_INDEX, server_cnt):
            # update_br_agent
            svbkutl=svbk_utls.svbk_utls()
            ret =svbkutl.update_br_agent(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
            if NG == ret[0]:
                msg='update_br_agent backup [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]

            #copy br.org_update for fast
            ret =svbkutl.update_br_org(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
            if NG == ret[0]:
                msg='copy br.org_update [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]

            #exec br_agent
            cmd='ssh root@%s /boot/%s %s %s b %s %s %s %s' %(server_info[i][IP_INDEX], svbkutl.get_br_agent_name(), server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME, server_info[STORAGE_SV][IP_INDEX], server_info[STORAGE_SV][USER_INDEX],  server_info[STORAGE_SV][PW_INDEX])
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
            if ret!=0:
                msg='make run backup  [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]



        ################
        #B Start status check
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

        while 1:
            all_ret=0

            ################
            #B time cal
            ################

            #end time get
            time2 = time.time()
            timedf = time2-time1

            timedf_int = int(timedf)
            HH= timedf_int / 3600
            #SS= timedf_int % 3600
            MM= (timedf_int % 3600)/60
            SS= timedf_int % 60

            #logout
            self.br_log(node_id, CLSTER_NAME, br_mode, "#### Loop  status Check Total Time: %s:%s:%s (h:m:s)" %(HH,MM,SS) )

            for i in range(START_INDEX, server_cnt):

                ################
                #B status loop check
                ################

                #eval ssh root@\${STORAGE_SV[0]} grep -wq \${B_STATUS[2]} "\${SAVE_DIR_NAME}/status_b_*_\${SV$i[0]}" 2> /dev/null
                #tmp_ret=$?
                #ret=`expr $tmp_ret + $ret`
                #cmd='ssh root@%s grep -wq %s %s/status_b_*_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_END_STATUS, SAVE_DIR_NAME, server_info[i][IP_INDEX])
                cmd='ssh root@%s grep -wq %s %s/status_b_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_END_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                #self.logger.debug('server_info[%s] IP=%s SV_NAME=%s ret=%s' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX], ret) )
                self.b_log(node_id, CLSTER_NAME, 'server_info[%s] IP=%s SV_NAME=%s ' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )

                all_ret = ret + all_ret

                #cmd='ssh root@192.168.1.104 grep -wq backup_ok /backup/1217_1612/server/status_b_*_192.168.1.192'

                ################
                #B status view
                ################
                #cmd='ssh root@%s tail -n 1 %s/status_b_*_%s  2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][IP_INDEX])
                cmd='ssh root@%s tail -n 1 %s/status_b_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec_br_state(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

                ################
                #B status ng check
                ################
                #cmd='ssh root@%s grep -wq %s %s/status_b_*_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_NG_STATUS, SAVE_DIR_NAME, server_info[i][IP_INDEX])
                cmd='ssh root@%s grep -wq %s %s/status_b_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_NG_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                if ret==0:
                    self.br_log(node_id, CLSTER_NAME, br_mode, '#### Status loop check [backup_ng] Err IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                    #return self._fail(msg='Status loop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                    msg='Status loop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX])
                    return [NG, msg]

                #STATUS=`ssh root@${STORAGE_SV[0]} tail -n 1 "$SAVE_DIR_NAME/status_b_*_${SV1[0]}"  2> /dev/null`

                time.sleep(self.interval_time )

            if all_ret==0:
                break

            #force-stop
            if os.path.exists(FILEDIR+B_STOP_FILENAME):
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### force stop')
                os.remove(FILEDIR+B_STOP_FILENAME)
                msg='#### force stop '
                return [NG, msg]

                break

            #time-out
            if HH >= self.loop_timeout_m :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### HH is %s hour Over stop' %(self.loop_timeout_m))
                msg='#### HH is %s hour Over stop ' %(self.loop_timeout_m)
                return [NG, msg]
                break

        #################
        #set DB backup data
        #################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data  ')
        ret = self.setdb_backup_data(CLSTER_NAME, node_id, br_mode, kwargs['backup_name'], server_node_name, switch_node_name)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data ng ')
            msg='#### set DB backup data ng '
            return [NG, msg]

        #################
        #File & DB Delete
        #################
        ret=self.delete_data_and_dblist_opencenter(node_id, CLSTER_NAME, br_mode, server_info, SAVE_DIR_NAME, EXEC_USER)
        if 0 != ret:
            msg='#### delete db and data  ng '
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return [NG, msg]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')

        return [OK, 'ok'] #END


    def restore_cluster(self, api, node_id, **kwargs):

        try:
            br_mode="r"
            CLSTER_NAME = self.get_node_name(api, node_id)

            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            restore_folder_name = self.get_restore_foldername(**kwargs)

            if not restore_folder_name:
                self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder name is none')
                return [NG, '###backup folder name is none']

            self.make_log_file_name(CLSTER_NAME,node_id,br_mode,**kwargs)

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            ret = self.br_precheck(api, node_id, br_mode, CLSTER_NAME)
            if ret==1:
                return [OK, "ok"]
            elif ret==-1:
                msg='#### backup runnning then err'
                return [NG, msg]
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')

            ###############
            ####Restore####
            ###############
            retArray = self.restore_cluster_sub(api, node_id, **kwargs)

            #set mode none
            self.set_mode_state(api, node_id, MODE_NONE)

            return retArray

        except Exception,e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()) )

            #set mode none
            self.set_mode_state(api, node_id, MODE_NONE)
            raise

    #####################
    #Restore Module
    #####################
    def restore_cluster_sub(self, api, node_id, **kwargs):

        #####################
        #R predefine 
        #####################

        br_mode="r"
        CLSTER_NAME = self.get_node_name(api, node_id)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Restore Start ')

        #self.b_log(node_id, CLSTER_NAME, 'backup_cluster start ')

        if not 'restore_name' in kwargs:
            return [NG, 'backup folder name is required']

        #get restore folder name
        restore_folder_name = self.get_restore_foldername(**kwargs)

        BACKUP_FOLDER_RNAME=CLSTER_NAME + "_ID%s" %(node_id) +"/" + restore_folder_name 

        self.br_log(node_id, CLSTER_NAME, br_mode, 'backup_name: %s' % BACKUP_FOLDER_RNAME)

        #self.logger.debug('NOVA_CLUSTER_NAME=%s' %(CLSTER_NAME))

        ret=self.set_system_param(CLSTER_NAME,node_id,br_mode)
        if ret!=0:
            #self.logger.debug('backup exec start err all=%d index=%d' %(server_cnt, i) )
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
            msg='#### set_system_param err'
            return [NG, msg]

        #####################
        #R define
        #####################

        SAVE_DIR_NAME=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/server"
        SAVE_DIR_NAME_SWITCH=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/switch"

        #SERVER_INFO_FILE_NAME="serv_info"
        #SERVER_LIST_FILE=SAVE_DIR_NAME+"/"+SERVER_INFO_FILE_NAME

        NODE_INFO_FILE_NAME="node_info"
        NODE_LIST_FILE=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, **kwargs)

        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return [NG, '###get token ng']

        #start time get
        time1 = time.time()

        ######################
        #R Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        #make storage server save val
        server_cnt=1
        server_info_num=4
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ########################
        #R Set Storage Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        #retdata = self.set_server_info(node_id, server_info[0],STORAGE_SERVER_NAME, CLSTER_NAME, br_mode)
        retdata = self.set_server_info(node_id, server_info[0],self.storage_server_name, CLSTER_NAME, br_mode)

        if 0 != retdata[0]:
            #self.logger.debug('server info set err')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
            #return self._fail(msg='server info set err')
            msg='server info set err'
            return [NG, msg]

        ################
        #R Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
#        EXEC_USER=server_info[STORAGE_SV][USER_INDEX]
        ret = self.get_user_name(self.opencenter_server_name)
        if 0 == ret[0]:
            EXEC_USER=ret[1]
        else:
            self.b_log(node_id, CLSTER_NAME, '#### Set Exec_User error' )
            msg='Set Exec_User error'
            return [NG, msg]

        #####################################
        #R Set Storage Server info
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        i=STORAGE_SV
        self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start  index=%d' %( i))
        ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err index=%d' %( i) )
            msg='make_exec err'
            return [NG, msg]

        #######################
        #R Get backup ServerInfo
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Get backup ServerInfo ')

        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")
        file_path='/tmp/' + NODE_INFO_FILE_NAME + tm

        cmd="scp  root@%s:%s %s  " %(server_info[STORAGE_SV][IP_INDEX] , NODE_LIST_FILE, file_path)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            NODE_LIST_FILE
            msg='R node_info trans err'
            return [NG, msg]

        f=open(file_path, 'r')
        backup_node_info = json.load(f)
        f.close()

        cmd="rm %s " %(file_path)
        commands.getoutput(cmd)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Get backup ServerInfo :%s' %(backup_node_info))

        ########################
        #R Get server name for opencenterDB
        #######################
        #self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for opencenterDB')
        #ret_info=self.get_node_info(api, node_id, CLSTER_NAME, br_mode)
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for storage server')
        ret_restore_info = self.get_node_info_from_serverFile(api, node_id, CLSTER_NAME, br_mode, backup_node_info)

        server_node_name=ret_restore_info[SV_NAME]
        server_num=len(server_node_name)
        server_cnt=server_num+1

        tmp_server_info = server_info[STORAGE_SV]
        ###################
        #resize server info
        ###################
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]
        server_info[STORAGE_SV] = tmp_server_info

        switch_node_name = ret_restore_info[SW_NAME]
        switch_num=len(switch_node_name)
        switch_node_name_char=','.join(switch_node_name)

        if (0 == server_num) and ( 0 == switch_num):
            #self.logger.debug('sever num = 0 then not backup')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
            return [OK, 'ok']

        ########################
        #R Check Cluster node
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Check Cluster node')

        backupedCluster_node = server_node_name + switch_node_name

        ret = self.check_novacluster_node(CLSTER_NAME,node_id,br_mode, backupedCluster_node,api)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#R Check Cluster node  check ng')
            msg='R Check Cluster node  check ng'
            return [NG, msg]

        ########################
        #R Set NovaClaster Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server info')

        for i in range(1, server_cnt):
            #self.logger.debug('ddd i=%s server_node_name=%s' %(i, server_node_name[i-1]))
            self.br_log(node_id, CLSTER_NAME, br_mode, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )

            retdata = self.set_server_info(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
            if 0 != retdata[0]:
                #self.logger.debug('Set Server info set server_node_name=%s' %(server_node_name[i-1]))
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )

                #return self._fail(msg='Set Server info set server_node_name=%s' %(server_node_name[i-1]))
                msg='Set Server info set server_node_name=%s' %(server_node_name[i-1])
                return [NG, msg]

        #####################################
        #R Set NovaClaster Server Make Exec Env
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server Make Exec Env')

        for i in range(1, server_cnt):
            #self.logger.debug('backup exec start all=%d index=%d' %(server_cnt, i) )
            self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start all=%d index=%d' %(server_cnt, i))
            ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
            if ret!=0:
                #self.logger.debug('backup exec start err all=%d index=%d' %(server_cnt, i) )
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err all=%d index=%d' %(server_cnt, i) )

                #return self._fail(msg='make_exec err')
                msg='make_exec err'
                return [NG, msg]

        #######################
        #R Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        #ssh root@192.168.1.193 ls -d  /backup/01071058
        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory Err %s is no directory' %(SAVE_DIR_NAME))
            #return self._fail(msg='Check Directory ')
            msg='Check Directory '
            return [NG, msg]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### parent_id change')

        server_node_id     = ret_restore_info[SV_NODE]
        switch_node_id     = ret_restore_info[SW_NODE]
        server_p_node_list = ret_restore_info[SV_P_NODE]
        switch_p_node_list = ret_restore_info[SW_P_NODE]

        node_id_list = server_node_id + switch_node_id
        parent_id_list = server_p_node_list + switch_p_node_list
        node_name_list = server_node_name   + switch_node_name

        #DDD debug DDD
        #switch_num=0

        #DDD debug
        if DEBUG == "ON":
            switch_num=0
        #################
        #R switch restore
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Switch restore Start')

            #psbk=psbk_manager.psbk_manager(STORAGE_SERVER_NAME, SAVE_DIR_NAME_SWITCH)
            #psbk=psbk_manager.psbk_manager(self.storage_server_name, SAVE_DIR_NAME_SWITCH)
            psbk=psbk_manager.psbk_manager(EXEC_USER, self.storage_server_name, SAVE_DIR_NAME_SWITCH,self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH Call psbk.set_PS_list')

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_name_char))

            ret=psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH psbk.set_PS_list Err')

                #return self._fail(msg='#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)')
                msg='#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)'
                return [NG, msg]

            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
            ret=psbk.exec_restore()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
                #return self._fail(msg='####SWITCH  psbk.exec_restore() ')
                msg='####SWITCH  psbk.exec_restore() '
                return [NG, msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch restore End')

        #DDD
        if DEBUG == "ON":
            server_num = 0

        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            #return self._ok()
            return [OK, 'ok']

        #######################
        #R Dell Restore Status File
        #######################
        #ssh root@${STORAGE_SV[0]} rm -rf "$SAVE_DIR_NAME/status_${mode}_*"  2> /dev/null
        cmd='ssh root@%s rm -rf %s/status_r_*  2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Dell Restore Status File Err ')
            
            #return self._fail(msg='Dell Restore Status File Err ')
            msg='Dell Restore Status File Err '
            return [NG, msg]

        ################
        #R Run Restore
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Restore2')

        for i in range(START_INDEX, server_cnt):
            # update_br_agent
            svbkutl=svbk_utls.svbk_utls()
            ret = svbkutl.update_br_agent(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
            if NG == ret[0]:
                msg='update_br_agent backup [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]

            #copy br.org_update for fast
            ret =svbkutl.update_br_org(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
            if NG == ret[0]:
                msg='copy br.org_update [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]

            cmd='ssh root@%s /boot/%s %s %s r %s %s %s %s' %(server_info[i][IP_INDEX], svbkutl.get_br_agent_name(), server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME, server_info[STORAGE_SV][IP_INDEX], server_info[STORAGE_SV][USER_INDEX],  server_info[STORAGE_SV][PW_INDEX])
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
            if ret!=0:
                msg='make run resotre  [%s] err ' % (server_info[i][IP_INDEX])
                self.br_log(node_id, CLSTER_NAME, br_mode, msg )
                return [NG, msg]

        ################
        #R Start status check
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

        while 1:
            all_ret=0

            ################
            #R time cal
            ################

            #end time get
            time2 = time.time()
            timedf = time2-time1

            timedf_int = int(timedf)
            HH= timedf_int / 3600
            #SS= timedf_int % 3600
            MM= (timedf_int % 3600)/60
            SS= timedf_int % 60

            #logout
            self.br_log(node_id, CLSTER_NAME, br_mode, "#### Loop  status Check Total Time: %s:%s:%s (h:m:s)" %(HH,MM,SS) )

            for i in range(START_INDEX, server_cnt):

                ################
                #R status loop check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_END_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                self.br_log(node_id, CLSTER_NAME, br_mode, 'server_info[%s] IP=%s SV_NAME=%s ' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )

                all_ret = ret + all_ret

                ################
                #R status view
                ################
                cmd='ssh root@%s tail -n 1 %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec_br_state(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

                ################
                #R status ng check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_NG_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                if ret==0:
                    self.br_log(node_id, CLSTER_NAME, br_mode, '#### Status loop check [backup_ng] Err IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                    msg='Status loop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX])
                    return [NG, msg]

                time.sleep(self.interval_time )

            if all_ret==0:
                break

            #force-stop
            if os.path.exists(FILEDIR+R_STOP_FILENAME) :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### force stop')
                os.remove(FILEDIR+R_STOP_FILENAME)
                msg='#### force stop '
                return [NG, msg]

            #time-out
            if HH >= self.loop_timeout_m :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### HH is %s hour Over stop' %(self.loop_timeout_m))
                msg='#### HH is %s hour Over stop ' %(self.loop_timeout_m)
                return [NG, msg]

        ########################
        #R Set_Parent_id
        ########################
        #self.set_parent_id_all( CLSTER_NAME, node_id, br_mode, api, node_id_list, parent_id_list)
        self.set_parent_id_all( CLSTER_NAME, node_id, br_mode, api,node_name_list , parent_id_list)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')

        return [OK, 'ok']

