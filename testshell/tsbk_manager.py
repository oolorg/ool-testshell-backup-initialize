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
import svbk_utls
import svbk_conflict
import pxssh
import fuel_utls
#import ipmi_client
from retry import retry

# Physical Switch Backup/Restore Manager
# disk size
LIMIT_DISK_SIZE_G=1
INTERVAL_TIME=10
CANCEL="off"

CONFIG_FILE = 'settings.ini'


NAME_INDEX = 0
IP_INDEX = 1
USER_INDEX = 2
PW_INDEX = 3
C_IP_INDEX = 4

CLONEZILLA_SV = 0
START_INDEX = 1


R_END_STATUS="restore_ok"
R_NG_STATUS="restore_ng"

B_END_STATUS="backup_ok"
B_NG_STATUS="backup_ng"

#FILEDIR="/etc/backuprestore"
FILEDIR = "/etc/ool_br_rest"
BASE_DIR_NAME="/backup"
CLONEZILLA_IMG_DIR = "/backup/partimag"

OK=0
NG=1

#DEBUG="ON"
DEBUG="OFF"

B_STOP_FILENAME="/b_stop"
R_STOP_FILENAME="/r_stop"
ROOP_TIME_OUT=60
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


###############################
#Server Backup Up Manager
###############################
class tsbk_manager:
    def __init__(self, logger):

        self.logObj = logger
        self.limit_disk_size = LIMIT_DISK_SIZE_G
        self.interval_time = INTERVAL_TIME
        self.loop_timeout_m = ROOP_TIME_OUT
        self.opencenter_server_name = ""
        self.storage_server_name = ""
        self.clonezilla_server_name = ""
        self.restore_maxFolderNum = RESTORE_MAX_INDEX
        self.logpath = "/var/log/br/"
        self.logfile = "default.log"
        self.token = ""
        self.date_time = datetime.datetime.today()
        self.folder_date_str = self.date_time.strftime("%Y_%m%d_%H%M%S---")

    def __del__( self ):
        del self.logObj
        pass

    def make_log_file_name(self, clster_name, node_id, br_mode, **kwargs):
        ret=0#
        d = self.date_time
        tm1= d.strftime("%m%d_%H%M_%S_")

        if "b" == br_mode :
            self.logfile=tm1 + br_mode +"_" + clster_name + "_ID%s_" %(node_id) \
                                   + "N"+ self.folder_date_str + kwargs['backup_name'] + ".log"
        else:
            self.logfile=tm1 + br_mode +"_" + clster_name + "_ID%s_" %(node_id) \
                                   + "N"+ self.folder_date_str + kwargs['restore_name'] + ".log"
        return 0

    def make_log_file_name_restore(self, clster_name, node_id, br_mode):
        ret=0
        d = self.date_time
        tm1= d.strftime("%m%d_%H%M_%S_")

        self.logfile=tm1 + br_mode +"_" + clster_name + "_ID%s_" %(node_id) \
                               + "N"+ self.folder_date_str + ".log"

        return 0

    def br_log(self, node_id, name, br_mode, log):

        self.logObj.debug('BRLOG ID=%s NAME=%s %s:%s' %(node_id, name, br_mode, log))

        d = datetime.datetime.today()
        tm= d.strftime("%m%d %H:%M:%S")

        f = open(self.logpath+self.logfile, 'a+')
        f.write('%s BRLOG ID=%s NAME=%s %s:%s \n' %(tm, node_id, name, br_mode, log))
        f.close()

        return 0

    def set_system_param(self, clster_name, node_id, br_mode):

        conf = ConfigParser.SafeConfigParser()

        set_file_path = FILEDIR + "/" + CONFIG_FILE

        ret = conf.read(set_file_path)

        if len(ret) == 0:
            self.br_log(node_id, clster_name, br_mode, '####set_system_param ng file is nothing ')
            return NG

        self.br_log(node_id, clster_name, br_mode, '####set_system_param file_name :%s' % (ret[0]))

        self.limit_disk_size = int(conf.get('options', 'limit_disk_size'))
        self.interval_time = int(conf.get('options', 'interval_time'))
        self.opencenter_server_name = conf.get('options', 'opencenter_server_name')
        self.storage_server_name = conf.get('options', 'storage_server_name')
        self.clonezilla_server_name = conf.get('options', 'clonezilla_server_name')
        self.restore_maxFolderNum = int(conf.get('options', 'restore_maxFolderNum'))
        self.loop_timeout_m = int(conf.get('options', 'loop_timeout_m'))

        self.br_log(node_id, clster_name, br_mode,
                    '####read_file limit_disk_size :%s' % (self.limit_disk_size))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file interval_time :%s' % (self.interval_time))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file opencenter_server_name :%s' % (self.opencenter_server_name))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file storage_server_name :%s' % (self.storage_server_name))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file clonezilla_server_name :%s' % (self.clonezilla_server_name))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file restore_maxFolderNum :%s' % (self.restore_maxFolderNum))
        self.br_log(node_id, clster_name, br_mode,
                    '####read_file loop_timeout_m :%s' % (self.loop_timeout_m))

        return OK

    def set_token(self,clster_name,node_id,br_mode, token):

        if len(token) == 0:
            self.br_log(node_id, clster_name, br_mode, '###token is none ' )
            return 1

        self.token = token

        #self.br_log(node_id, clster_name, br_mode, '###token is : %s' % self.token)

        return 0

    def get_node_info(self, node_id, name, br_mode,node_name):

        #ZANTEI set node

        #get all node id
        server_node_name   = []
        switch_node_name   = []

        #server_node_name=node_name

        utls=svbk_utls.svbk_utls()
        utls.set_auth(self.token)
        #utls.set_auth("aaa")
        retArray = utls.separate_node(node_name)

        if( isinstance(retArray[0],list) == False or isinstance(retArray[1],list)== False ):
            self.br_log(node_id, name, br_mode, '*** utls.separate_node  retArray: %s' % retArray)
            return  [1, server_node_name, switch_node_name]

        server_node_name = retArray[0]
        switch_node_name = retArray[1]


        if( len(node_name) != len(server_node_name+switch_node_name)):
            self.br_log(node_id, name, br_mode, '*node different err node_name:%s server_node_name:%s  switch_node_name%s'
             %(node_name, server_node_name,switch_node_name))
            return  [1, server_node_name, switch_node_name]

        self.br_log(node_id, name, br_mode, '*** Resouce Manager DB SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** Resouce Manager DB SWITCH_NODE_NAME  : %s' % switch_node_name)


        return  [0, server_node_name, switch_node_name]

        

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

    def set_server_info(self, node_id, server_info, server_name, name, br_mode):

        #make Resource Manager Instance
        ori = ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        self.br_log(node_id, name, br_mode, "ddddd self.token:%s" % (self.token))

        #get IP address
        data = ori.get_nic_traffic_info(server_name, 'M-Plane')
        data_c = ori.get_nic_traffic_info(server_name, 'M2-Plane')
        if (-1 != data[0]) and (-1 != data_c[0]):
            server_info[IP_INDEX] = data[1][0]['ip_address']
            server_info[C_IP_INDEX] = data_c[1][0]['ip_address']
        else:
            # todo
            self.br_log(node_id, name, br_mode, "nic traffic_type error:%s" % (data[1]))
            ret = 1
            return ret

        #get username password
        data = ori.get_device(server_name)
        ret = 0

        if -1 != data[0]:

            #input server info
            server_info[NAME_INDEX] = server_name
            server_info[USER_INDEX] = data[1]['user_name']
            server_info[PW_INDEX] = data[1]['password']

            #check info (length)
            for i in range(0, 4):
                self.br_log(
                    node_id, name, br_mode,
                    "set_server_info temp_server_info[%s]=%s" % (i, server_info[i]))
                char_len = len(server_info[i])
                if char_len == 0:
                    self.br_log(node_id, name, br_mode, "set_server_info len=0 err ")
                    ret = 1
                    break
        else:
            self.br_log(
                node_id, name, br_mode, "set_server_info <device error:+ data[0]=%s " % (data[0]))
            ret = 1

        return ret


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

#    def _ssh_copyid(self, exec_user,DEST_IP, DEST_USER, DEST_PW):
    def _ssh_copyid(self, exec_user,br_mode, node_id, name, DEST_IP, DEST_USER, DEST_PW):
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
        #print p.before # print out the result
        self.br_log(node_id, name, br_mode, 'ssh_copy_id:%s' %(p.before) )

        return 0


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

    def make_exec(self, exec_user, DEST_IP, DEST_USER, DEST_PW, FILEDIR, br_mode, node_id, name):


        #known host key_delete
        cmd='ssh-keygen -f "/home/%s/.ssh/known_hosts" -R %s' %(exec_user, DEST_IP)

        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)


        #key_generate
        flug = os.path.exists("/home/%s/.ssh/id_rsa" %(exec_user))
        if not flug:
            self.br_log(node_id, name, br_mode, 'Plese make /home/%s/.ssh/id_rsa IP=%s' %(exec_user, DEST_IP) )

            return ret

        #ssh_copy_id
        self.br_log(node_id, name, br_mode, 'ssh_copy_id exec IP=%s' %(DEST_IP) )

        #print "make_exec"

        #self._ssh_copyid(exec_user,DEST_IP, DEST_USER, DEST_PW)
        self._ssh_copyid(exec_user,br_mode, node_id,  name, DEST_IP, DEST_USER, DEST_PW)

        #print "make_exec end"

        return 0


    def check_inputdata(self, node_id, name, br_mode, domain, topology_name):

        ################
        #B Input DataCheck
        ################
        ngchar_list=["|"," "]

        for char in ngchar_list:
            ret1=domain.find(char)
            if ret1 != -1:
                self.br_log(node_id, name, br_mode, '#### Input DataCheck Err domain,space or pipe:[%s] ' %(domain))
                return [1, '#### Input DataCheck Err domain=[%s],space or pipe ' %(domain)]

            ret2=topology_name.find(char)
            if ret2 != -1:
                self.br_log(node_id, name, br_mode, '#### Input DataCheck Err topology_name,space or pipe:[%s] ' %topology_name)
                return [1, '#### Input DataCheck Err topology_name=[%s],space or pipe ' %(topology_name)]

        return [0, "ok"]


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

    def setdb_backup_data2(self,db_clster_name,db_folder_name,db_node_list):

        #make regist info
        #clastername_ID8
        #db_clster_name=  clster_name + "_ID%s" %(node_id)

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        #db_server_list = server_list
        #db_switch_list = switch_list

        #server & switch
        #db_node_list = db_server_list + db_switch_list


        #folder_name
        #d = datetime.datetime.today()
        #db_folder_name= self.folder_date_str  + folder_name

        print('#### set_backup_data :  db_clster_name = %s' %(db_clster_name))
        print('#### set_backup_data :  db_folder_name = %s' %(db_folder_name))
        print('#### set_backup_data :  db_node_list = %s'   %(db_node_list))

        #set db
        data=ori.set_backup_data(db_clster_name, db_folder_name, db_node_list)


        if -1 == data[0]:
            print('#### set_backup_data err  data=%s' %(data))
            return 1

        print('#### success data=%s' %(data))

        return 0

    def setdb_backup_data(self, clster_name, node_id,
                          br_mode, folder_name, server_list, switch_list):

        #make regist info
        #clastername_ID8
        db_clster_name = clster_name + "_ID%s" % (node_id)

        ori = ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        db_server_list = []
        for server_name in server_list:
            data_work = ori.get_node(server_name)
            if -1 != data_work[0]:
                server_data = {'device_name': server_name.encode('utf-8'),
                               'server_type': data_work[1]["server_type"].encode('utf-8')}
                db_server_list.append(server_data)

        db_switch_list = []
        for switch_name in switch_list:
            swtich_data = {'device_name': switch_name.encode('utf-8')}
            db_switch_list.append(swtich_data)

        # server & switch
        db_node_list = db_server_list + db_switch_list

        # folder_name
        # d = datetime.datetime.today()
        db_folder_name = self.folder_date_str + folder_name

        self.br_log(node_id, clster_name, br_mode,
                    '#### set_backup_data :  db_clster_name = %s' % (db_clster_name))
        self.br_log(node_id, clster_name, br_mode,
                    '#### set_backup_data :  db_folder_name = %s' % (db_folder_name))
        self.br_log(node_id, clster_name, br_mode,
                    '#### set_backup_data :  db_node_list = %s' % (db_node_list))

        # set db
        data = ori.set_backup_data(db_clster_name, db_folder_name, db_node_list)

        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode,
                        '#### set_backup_data err  data=%s' % (data))
            return 1

        return 0

    def setdb_server_type(self, clster_name, node_id, br_mode, restore_name):

        # make clastername_ID
        db_clster_name = clster_name + "_ID%s" % (node_id)

        ori = ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        data = ori.get_backup_query(cluster_name=db_clster_name, backup_name=restore_name)

        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode,
                        " ori.get_backup_cluster err data=%s  db_clster_name=%s"
                        % (data, db_clster_name))
            return 1

        # get backup device info
        devices_data = data[1][0]['devices']

        for device in devices_data:
            if 'server_type' in device:
                ret = ori.mod_node_data(
                        device['device_name'],
                        {'server_type': device['server_type'].encode('utf-8')})

                if -1 == ret[0]:
                    self.br_log(node_id, clster_name, br_mode,
                                '#### mod_node_data err ret=%s' % (ret))
                    return 1

        return 0

    def make_nodeinfo_file(self, clster_name, node_id, br_mode, backup_node_data_info):

        #define node list
        server_node_name   = backup_node_data_info[SV_NAME]
        switch_node_name   = backup_node_data_info[SW_NAME]


        server_num = len(server_node_name)

        switch_num = len(switch_node_name)

        jdata ={"server": [],"switch": []}

        #set server node
        for i in range(server_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(server_node_name[i])}
            jdata[BACKUP_DATA_SV].append(tmp)

        #set switch node
        for i in range(switch_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(switch_node_name[i])}
            jdata[BACKUP_DATA_SW].append(tmp)


        #file name make
        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")
        TMP_FILE='/tmp/node_info' + tm

        #file OutPut
        with open(TMP_FILE, 'w') as f:
            json.dump(jdata, f, sort_keys=True, indent=4)

        return [0, TMP_FILE]

    #####################
    #Backup Module
    #####################
    def backup_cluster_sub(self, node_id, token, topology_name, domain, node_name, **kwargs):

        #####################
        #B predefine
        #####################
        br_mode = "b"
        CLSTER_NAME = "TS_" + domain + "_" + topology_name

        ret = self.check_inputdata(node_id, CLSTER_NAME, br_mode, domain, topology_name)

        if ret[0] != 0:
            return ['NG', ret[1]]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Backup Start ')

        if not 'backup_name' in kwargs:
            return ['NG', 'backup folder name is required']

        BACKUP_FOLDER_RNAME = CLSTER_NAME + \
            "_ID%s" % (node_id) + "/" + self.folder_date_str + kwargs['backup_name']
        BACKUP_IMGNAME_PRE = self.folder_date_str + kwargs['backup_name']

        self.br_log(node_id, CLSTER_NAME, br_mode, '###  Input Parameter  ####')
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_id              : %s' % node_id)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'token                : %s' % token)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'topology_name        : %s' % topology_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'domain               : %s' % domain)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_name            : %s' % node_name)
        self.br_log(node_id, CLSTER_NAME, br_mode,
                    'kwargs[backup_name]  : %s' % (kwargs['backup_name']))

        self.br_log(node_id, CLSTER_NAME, br_mode, 'backup_folder_name: %s' % BACKUP_FOLDER_RNAME)

        ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
            msg = '#### set_system_param err'
            return ['NG', msg]

        #####################
        #B define
        #####################
        SAVE_DIR_NAME = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/server"
        SAVE_DIR_NAME_SWITCH = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/switch"

        NODE_INFO_FILE_NAME = "node_info"
        NODE_LIST_FILE = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, token)

        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return ['NG', '###get token ng']

        ##################
        #check delete data num
        ##################

        #get data
        db_clster_name = "TS_" + domain + "_" + topology_name + "_ID0"
        getArray = self.get_restore_folder_list(node_id, CLSTER_NAME, br_mode, db_clster_name)

        #check data num
        if (getArray[0] == 0) and (len(getArray) == 2):
            backupFolderList = getArray[1]
            backupdata_num = len(backupFolderList)

            if backupdata_num >= self.restore_maxFolderNum:
                msg = '#### err:backup Data Num is Max, ' + \
                    'Plese delete Backup Data by Cloud Shell delete function' + \
                    '::backupdata_num=%s max_backupdata_Num=%s' % (
                        backupdata_num, self.restore_maxFolderNum)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

        ########################
        #B Get server name for opencenterDB
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for opencenterDB')

        ret = self.get_node_info(node_id, CLSTER_NAME, br_mode, node_name)
        if ret[0] != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###Get server name for DB err')
            return ['NG', '###Get server name for DB err']

        ret_info = [ret[1], ret[2]]

        server_node_name = ret_info[SV_NAME]
        server_num = len(server_node_name)

        switch_node_name = ret_info[SW_NAME]
        switch_num = len(switch_node_name)
        switch_node_name_char = ','.join(switch_node_name)

        #server,switch node info
        backup_node_data_info = ret_info

        self.br_log(
            node_id, CLSTER_NAME, br_mode,
            '*** SERVER_NODE_LIST :List %s, cnt=%s' % (server_node_name, server_num))
        self.br_log(
            node_id, CLSTER_NAME, br_mode,
            '*** SWITCH_NODE_LIST :List %s, cnt=%s' % (switch_node_name, switch_num))

        if (0 == server_num) and (0 == switch_num):
            #self.logger.debug('sever num = 0 then not backup')
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### sever, switch num is 0 then "no action"')
            return ['OK', 'Backup ok']

        ######################
        #B Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        server_cnt = server_num+1
        server_info_num = 5
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ################
        #B Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
        ret = self.get_user_name(self.opencenter_server_name)
        if 0 == ret[0]:
            EXEC_USER = ret[1]
        else:
            self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
            msg = 'Set Exec_User error'
            return ['NG', msg]

        ########################
        #B Set Storage Server indfo env
        ########################
        ret, msg = self.set_server_info_env(
            node_id, CLSTER_NAME, br_mode,
            [self.clonezilla_server_name], server_info, 1, EXEC_USER)
        if OK != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
            return ['NG', msg]

        ########################
        #B Set NovaClaster Server info env
        ########################
        ret, msg = self.set_server_info_env(
            node_id, CLSTER_NAME, br_mode,
            server_node_name, server_info[1:], server_num, EXEC_USER)
        if OK != ret:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Set Server info  server_node_name=%s' % (server_node_name[i-1]))
            return ['NG', msg]

        #######################
        #B Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        cmd = 'ssh openstack@%s ls -d  %s 2> /dev/null' % (
            server_info[CLONEZILLA_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret == 0:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Check Directory Err (already same name dirctory)')
            msg = 'Check Directory (already same name dirctory)'
            return ['NG', msg]

        #################################
        #B Check Strorage Server Disk Size
        ################################
        cmd = "ssh openstack@%s df -k | grep '/backup' " % \
            (server_info[CLONEZILLA_SV][IP_INDEX]) + "| awk '{ print $4 }'"
        #print cmd
        ret_list = self.shellcmd_exec_rest_diskSize(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if 0 != ret_list[0]:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Check Strorage Server Disk Size Err =%s' % (ret_list[0]))

            msg = '#### Check Strorage Server Disk Size Err =%s' % (ret_list[0])
            return ['NG', msg]

        diskSize_G = ret_list[1]/(1024*1024)

        #chek disk size
        if self.limit_disk_size >= diskSize_G:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Check Strorage Server Disk Size Shortage Err " + \
                "limit_disk_size=%s > diskSize_G=%s ' % (self.limit_disk_size, diskSize_G))

            msg = '#### Check Strorage Server Disk Size Shortage Err " + \
                "limit_disk_size=%s > diskSize_G=%s ' % (self.limit_disk_size, diskSize_G)
            return ['NG', msg]

        self.br_log(
            node_id, CLSTER_NAME, br_mode,
            '#### Check Strorage SV Disk Size LIMIT(G)=%s diskSize(G)=%s ' % (
                self.limit_disk_size, diskSize_G))

        #########################################
        #B Make Backup directory to Storage Server
        #########################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Backup directory to Storage Server')
        cmd = '%s/dirmake  %s  %s  %s  %s' % (
            FILEDIR, BASE_DIR_NAME, BACKUP_FOLDER_RNAME,
            server_info[CLONEZILLA_SV][IP_INDEX], server_info[CLONEZILLA_SV][USER_INDEX])
        #print "cmd:" + cmd
        #print "EXEC_USER:" + EXEC_USER

        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### make dir err ')
            msg = 'make dir err'
            return ['NG', msg]

        #######################
        #B Make Directory Check
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Directory Check ')

        cmd = 'ssh openstack@%s ls -d  %s 2> /dev/null' % (
            server_info[CLONEZILLA_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Make Directory Check Err (directory is none)')
            msg = 'Make Directory Check Err (directory is none)'
            return ['NG', msg]

        ##########################
        #set backup info to storage
        ##########################
        ret = self.trans_node_info(
            node_id, CLSTER_NAME, br_mode,
            backup_node_data_info, server_info, EXEC_USER, NODE_LIST_FILE)
        if 0 != ret:
            msg = '#### set backup info to storage  ng'
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return ['NG', msg]

        #DDD debug DDD
        #switch_num=0

        #DDD
        if DEBUG == "ON":
            switch_num = 0
        #################
        #B switch backup
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup Start')

            psbk = psbk_manager.psbk_manager(
                EXEC_USER, self.clonezilla_server_name, SAVE_DIR_NAME_SWITCH, self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.set_PS_list')

            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '####SWITCH_NODE_LIST :List(char) %s ' % (switch_node_name_char))

            ret = psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.set_PS_list Err')
                msg = '####SWITCH  psbk.set_PS_list Err psbk.set_PS_list Err)'
                return ['NG', msg]

            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_backup()')
            ret = psbk.exec_backup()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_backup() Err')
                msg = '####SWITCH  psbk.exec_backup() Err'
                return ['NG', msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup End')

        #DDD
        if DEBUG == "ON":
            server_num = 0

        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            #return self._ok()
            return ['OK', 'Backup ok']

        #########################################################################
        ret = self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server Back UP Start')

        ################
        #B Run backup
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run backup2')

        clonezilla_info = {}
        clonezilla_info['ip_address'] = server_info[CLONEZILLA_SV][IP_INDEX]
        clonezilla_info['username'] = server_info[CLONEZILLA_SV][USER_INDEX]
        clonezilla_info['password'] = server_info[CLONEZILLA_SV][PW_INDEX]

        data_info = []
        for data in server_info[START_INDEX:]:
            tmp_data = {}
            tmp_data['hostname'] = data[NAME_INDEX]
            tmp_data['ip_address'] = data[IP_INDEX]
            tmp_data['username'] = data[USER_INDEX]
            tmp_data['password'] = data[PW_INDEX]
            tmp_data['ip_address_c'] = data[C_IP_INDEX]
            data_info.append(tmp_data)

        ret = fuel_utls.clonezilla_exec(self.token, fuel_utls.MODE_BACKUP,
                                        clonezilla_info, data_info,
                                        server_cnt - 1, BACKUP_IMGNAME_PRE)

        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Clonezilla server Backup Error')
            msg = 'Clonezilla server Backup Error'
            return ['NG', msg]

        for server in data_info:
            ret = self.wait_os_startup(
                server['hostname'], server['username'], server['password'])
            if ret == -1:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### wait_os_startup Error')

        #################
        #set DB backup data
        #################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data  ')
        ret = self.setdb_backup_data(
            CLSTER_NAME, node_id, br_mode,
            kwargs['backup_name'], server_node_name, switch_node_name)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data ng ')
            msg = '#### set DB backup data ng '
            return ['NG', msg]

        #################
        #File & DB Delete
        #################
        ret = self.delete_data_and_dblist(
            node_id, CLSTER_NAME, br_mode, topology_name,
            domain, server_info, SAVE_DIR_NAME, EXEC_USER)
        if 0 != ret:
            msg = '#### delete db and data  ng '
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return ['NG', msg]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')

        return ['OK', 'Backup ok']    # END

    @retry(pexpect.EOF, tries=100, delay=10)
    def wait_os_startup(self, hostname, username, password):

        #self.br_log(hostname, self.topology_name, "", '#### wait_os_startup Start')
        self.br_log(hostname, "", "", '#### wait_os_startup Start')

        # Get C-Plane/M-Plane address
        nicinfo = fuel_utls.node_nic_info(self.token, hostname)

        server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
        if (server_mip == -1):
            #self.br_log(hostname, self.topology_name, "", '#### wait_os_startup get M_PLANE Error')
            self.br_log(hostname, "", "", '#### wait_os_startup get M_PLANE Error')
            return -1

        # --- Set Public network env
        try:
            s = pxssh.pxssh()
            s.login(server_mip, username, password, login_timeout=10*60)
            s.logout()

        except pxssh.ExceptionPxssh, e:
            print "pxssh failed on login."
            print str(e)

        #self.br_log(hostname, self.topology_name, "", '#### wait_os_startup End')
        self.br_log(hostname, "", "", '#### wait_os_startup End')
        return 0

    def backup_cluster(self, node_id, token, topology_name, domain, node_name, **kwargs):

        try:
            br_mode = "b"
            CLSTER_NAME = domain + "_" + topology_name

            #conflict check method instance
            cflct = svbk_conflict.svbk_conflict()
            cflct.set_loger(self.logObj)

            # set conflict info

            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            #var = kwargs['backup_name']
            #if not var:
            #    self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder name is none')
            #    return [NG, '###backup folder name is none']

            self.make_log_file_name(CLSTER_NAME, node_id, br_mode, **kwargs)

            ###################
            #check mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            node_list = ','.join(node_name)
            ret = cflct.chk_mode_state("b", topology_name, node_list)
            if ret == 1:
                return ['NG', "backup already runnning"]
            elif ret == -1:
                msg = '#### restore runnning then err'
                return ['NG', msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')

            ###############
            ####Backup ####
            ###############
            retArray = self.backup_cluster_sub(
                node_id, token, topology_name, domain, node_name, **kwargs)

            #set mode none
            cflct.set_mode_state(topology_name, MODE_NONE)

            return retArray

        except Exception, e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :' + str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '#### trace  :%s' % (traceback.format_exc()))

            #set mode none
            cflct.set_mode_state(topology_name, MODE_NONE)

            #raise
            return ['NG', traceback.format_exc()]


##### RRR ####


    def get_restore_foldername(self, **kwargs):

        tmp_str = kwargs['restore_name']

        restore_folder_name = kwargs['restore_name']

        return restore_folder_name

    def get_node_info_from_serverFile(self, node_id, name, br_mode, node_info):

        #get all node id
        server_node_name   = []
        switch_node_name   = []

        for i in range(len(node_info[BACKUP_DATA_SV])):
            server_node_name.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_NAME])

        for i in range(len(node_info[BACKUP_DATA_SW])):
            switch_node_name.append(node_info[BACKUP_DATA_SW][i][BACKUP_DATA_KEY_NAME])

        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** storege server file SWITCH_NODE_NAME  : %s' % switch_node_name)

        retdata =[server_node_name, switch_node_name]

        return retdata

    def check_novacluster_node(self,clster_name,node_id,br_mode, backupedCluster_node,nowClaster_node):

        self.br_log(node_id, clster_name, br_mode, '#### R NovaCluster_CheckStart')

        self.br_log(node_id, clster_name, br_mode, '#### R server node check  backupedCluster_node:%s' %(backupedCluster_node ))
        self.br_log(node_id, clster_name, br_mode, '#### R server node check  nowClaster_node      %s' %(nowClaster_node ))

        set_ab=set(nowClaster_node)-set(backupedCluster_node)
        if 0 != len(set_ab):
            self.br_log(node_id, clster_name, br_mode, '#### R server node check Err unnecessary node:%s' %(set_ab))
            return 1

        self.br_log(node_id, clster_name, br_mode, '#### R NovaClaster_Cherck OK')

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
            return [1, data[1]]

        #get registed db list
        backupAllData = data[1]

        for i in range(len(backupAllData)):
            backup_list.append(backupAllData[i]["backup_name"])


        return [0, backup_list]


    def get_restore_folder_list(self,node_id, clster_name, br_mode, db_clster_name):

        #get db list
        retdata = self.get_restoredb_list(node_id, clster_name, br_mode,db_clster_name)
        if 0 !=retdata[0]:
            self.br_log(node_id, clster_name, br_mode, " get_restoredb_list err %s" %(retdata))
            return [1, retdata[1]]

        restore_list = retdata[1]

        #sort new data is top
        restore_list.sort(reverse=True)

        if len(restore_list) == 0:
            self.br_log(node_id, clster_name, br_mode, "restore_list is 0")
            return [1, "404"]

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

    def trans_node_info(self, node_id, CLSTER_NAME,
                        br_mode, backup_node_data_info, server_info, EXEC_USER, NODE_LIST_FILE):

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
        cmd = "scp %s openstack@%s:%s " %\
            (file_path, server_info[CLONEZILLA_SV][IP_INDEX], NODE_LIST_FILE)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### B backup info trans to strage err')
            return 1

        #remove file
        cmd = "rm %s " % (file_path)
        commands.getoutput(cmd)

        return 0

    def delete_dblist(self, node_id, Token, br_mode, topology_name, domain, input_index):

        try:
            br_mode = "del"
            CLSTER_NAME = "TS_" + domain + "_" + topology_name

            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            self.make_log_file_name_restore(CLSTER_NAME, node_id, br_mode)

            self.set_token(CLSTER_NAME, node_id, br_mode, Token)

            ##################
            #check Input folder index
            ##################
            #check index isdigit
            if (False == input_index.isdigit()):
                msg = '#### index is  wrong :not number :input_index=%s' % (input_index)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            ######################
            #B Make Server info list
            ######################
            server_cnt = 1
            server_info_num = 5
            server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

            ##################
            #check index range
            ##################
            ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
            if ret != 0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
                msg = '#### set_system_param err'
                return ['NG', msg]

            ########################
            #B Set Storage Server info
            ########################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

            retdata = self.set_server_info(
                node_id, server_info[0], self.clonezilla_server_name, CLSTER_NAME, br_mode)
            if 0 != retdata:
                msg = 'storege server info set err'
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            #num change
            index = int(input_index)

            ##################
            #get backup data list
            ##################
            db_clster_name = "TS_" + domain + "_" + topology_name + "_ID0"
            getArray = self.get_restore_folder_list(node_id, CLSTER_NAME, br_mode, db_clster_name)

            #fget check
            if (getArray[0] != 0) and (len(getArray) == 2):
                if getArray[1] == "404":
                    msg = '###restore data is none'
                    self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                    return ['NG', msg]

            #fget check ()
            if getArray[0] != 0:
                msg = '###You have not yet backup or restoreDB_list get err '
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            backupFolderList = getArray[1]
            backupdata_num = len(backupFolderList)

            ################
            #Set Exec_User
            ################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
            ret = self.get_user_name(self.opencenter_server_name)
            if 0 == ret[0]:
                EXEC_USER = ret[1]
            else:
                self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
                msg = 'Set Exec_User error'
                return ['NG', msg]

            ##################
            #check index range
            ##################
            if ((index > backupdata_num) or (index < 0)):
                msg = '#### index is wrong :index=%s backupdata_num=%s' % (index, backupdata_num)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            if (index == 0):
                msg = '#### index is wrong :start index is 0 (0 origin)'
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            ##################
            #delete data
            ##################
            return self.delete_backup_data(
                node_id, CLSTER_NAME, br_mode,
                db_clster_name, backupFolderList[index - 1], server_info, EXEC_USER)

        except Exception, e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '#### trace  :%s' % (traceback.format_exc()))

            return ['NG', traceback.format_exc()]

    def delete_backup_data(
            self, node_id, CLSTER_NAME, br_mode,
            db_clster_name, backupFolderList, server_info, EXEC_USER):

        rmdir = BASE_DIR_NAME + "/" + db_clster_name+"/"+backupFolderList
        rmimg = CLONEZILLA_IMG_DIR + "/" + backupFolderList + "*"

        self.br_log(node_id, CLSTER_NAME, br_mode,
                    '###Delete db data   backupFolderList=%s' % (backupFolderList))
        self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete dir=%s' % (rmdir))

        #################
        #db delete (db)
        #################
        ret = 0
        ret = self.delete_restoredb_list(
            node_id, CLSTER_NAME, br_mode, db_clster_name, backupFolderList)
        if ret != 0:
            msg = '###Delete db data  backupFolderList=%s err' % (backupFolderList)
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return ['NG', msg]

        #######################
        #db folder (data)
        #######################
        cmd = 'ssh openstack@%s rm -rf %s  > /dev/null' % \
            (server_info[CLONEZILLA_SV][IP_INDEX], rmdir)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            msg = '#### Del Restore Status File Err '
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return ['NG', msg]

        # fuel ----
        clonezilla_serverip = server_info[CLONEZILLA_SV][IP_INDEX]
        clonezilla_username = server_info[CLONEZILLA_SV][USER_INDEX]
        clonezilla_password = server_info[CLONEZILLA_SV][PW_INDEX]

        try:
            s = pxssh.pxssh()
            s.login(clonezilla_serverip, clonezilla_username, clonezilla_password)

            cmd = 'sudo rm -rf %s  > /dev/null' % (rmimg)

            s.sendline(cmd)
            s.expect('.*password for .*')
            s.sendline(clonezilla_password)
            s.prompt()

            logs = s.before
            #print "s.after:" + s.after
            #print "#s.before:" + s.before
            #print "s.buffer:" + s.buffer
            s.logout()

        except pxssh.ExceptionPxssh, e:
            print "pxssh failed on login."
            print str(e)

        if "rm:" in logs:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Del bakcup image File Err ')
            return ['NG', msg]

        return ['OK', "delete OK"]

    def get_dblist(self, node_id, Token,br_mode, topology_name, domain):

        db_clster_name = "TS_" + domain +"_" + topology_name + "_ID0"

        CLSTER_NAME = "TS_" + domain +"_" + topology_name 

        self.set_token(CLSTER_NAME, node_id, br_mode, Token)

        self.br_log(node_id, CLSTER_NAME, br_mode, '###get_dblist   topology_name=%s, domain=%s db_clster_name=%s' %(topology_name,domain,db_clster_name))

        #################
        #get retore folder name
        #################
        getArray = self.get_restore_folder_list(node_id, CLSTER_NAME, br_mode, db_clster_name)

        #folder serch :ng check
        if getArray[0] != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###backup data is none')
            #self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder get err')
            return ['NG', "###backup data is none"]

        data = getArray[1]

        #self.br_log(node_id, CLSTER_NAME, br_mode, '###backupFolderList=%s' %(data))

        restore_data_all=""

        #data length cheack
        if( len(data) <= 0 ):
            return ['OK', "###backup data is none len=%s" %(len(data))]
        else:
            for i in range(len(data)):
                msg="  Index: %s -> %s ::" %(i+1,data[i])
                restore_data_all=restore_data_all+msg

        self.br_log(node_id, CLSTER_NAME, br_mode, '###return msg =%s' %(restore_data_all))

        return ['OK', restore_data_all]


    def delete_data_and_dblist(self,node_id, CLSTER_NAME, br_mode, topology_name, domain, server_info, SAVE_DIR_NAME, EXEC_USER):

        self.br_log(node_id, CLSTER_NAME, br_mode, '###Delete Backup Data start')

        #################
        #get retore folder name
        #################
        db_clster_name = "TS_" + domain +"_" + topology_name + "_ID0"

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
        if( len(backupFolderList) <= self.restore_maxFolderNum  ):
            msg='###folder num is OK  len(backupFolderList)=%s restore_maxFolderNum=%s ' %(len(backupFolderList), self.restore_maxFolderNum)
            self.br_log(node_id, CLSTER_NAME, br_mode, msg)
            return 0

        ###################
        #delete DB & file
        ###################
        for i in range(self.restore_maxFolderNum  , len(backupFolderList) ):

            self.delete_backup_data(
                node_id, CLSTER_NAME, br_mode,
                db_clster_name, backupFolderList[i], server_info, EXEC_USER)


        return 0

    def restore_cluster_index(self, node_id, token, topology_name, domain, node_name, input_index):

        try:
            br_mode = "r"
            CLSTER_NAME = domain + "_" + topology_name

            #conflict check method instance
            cflct = svbk_conflict.svbk_conflict()
            cflct.set_loger(self.logObj)

            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            self.make_log_file_name_restore(CLSTER_NAME, node_id, br_mode)

            ##################
            #get system param
            ##################
            ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
            if ret != 0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
                msg = '#### set_system_param err'
                return ['NG', msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### input index=%s' % (input_index))

            ##################
            #check Input folder index
            ##################
            #check index isdigit
            if(False == input_index.isdigit()):
                msg = '#### index is  wrong :not number :input_index=%s' % (input_index)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            #num change
            index = int(input_index)

            #check index range
            if((index > self.restore_maxFolderNum) or (index <= 0)):
                msg = '#### index is wrong :index=%s restore_maxFolderNum=%s' %\
                    (index, self.restore_maxFolderNum)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            #################
            #get retore folder name
            #################
            db_clster_name = "TS_" + domain + "_" + topology_name + "_ID0"

            self.set_token(CLSTER_NAME, 0, "r", token)
            getArray = self.get_restore_folder_list(node_id, CLSTER_NAME, br_mode, db_clster_name)

            #folder serch :ng check
            if getArray[0] != 0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder get err')
                return ['NG', "###backup folder get err"]

            backupFolderList = getArray[1]

            #################
            #folder num check
            #################
            if(len(backupFolderList) < index):
                msg = '###folder is none len(backupFolderList)=%s restore_maxFolderNum=%s ' %\
                    (len(backupFolderList), self.restore_maxFolderNum)
                self.br_log(node_id, CLSTER_NAME, br_mode, msg)
                return ['NG', msg]

            restoreFolder = backupFolderList[index-1]

            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '#### backupFolderList :%s' % backupFolderList)
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### restoreFolder :%s' % restoreFolder)

            ###################
            #checck mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            node_list = ','.join(node_name)
            ret = cflct.chk_mode_state("r", topology_name, node_list)
            if ret == 1:
                return ['NG', "resotre already runnning"]
            elif ret == -1:
                msg = '#### backup runnning then err'
                return ['NG', msg]
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')

            ###############
            ####Restore####
            ###############
            retArray = self.restore_cluster_sub(
                node_id, token, topology_name, domain, node_name, restore_name=restoreFolder)

            #set mode none
            cflct.set_mode_state(topology_name, MODE_NONE)

            return retArray

        except Exception, e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :' + str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '#### trace  :%s' % (traceback.format_exc()))

            #set mode none
            cflct.set_mode_state(topology_name, MODE_NONE)

            #raise
            return ['NG', traceback.format_exc()]

    """
    #def restore_cluster(self, api, node_id, **kwargs):
    def restore_cluster(self, node_id, token, topology_name, domain, node_name,**kwargs):

        try:
            br_mode="r"
            CLSTER_NAME = domain + "_"+ topology_name

            #conflict check method instance
            cflct=svbk_conflict.svbk_conflict()
            cflct.set_loger(self.logObj)


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

            ###################
            #check mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            node_list=','.join(node_name)
            ret = cflct.chk_mode_state( "r", topology_name, node_list)
            if ret==1:
                return [NG, "resotre already runnning"]
            elif ret==-1:
                msg='#### backup runnning then err'
                return [NG, msg]
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')



            ###############
            ####Restore####
            ###############
            retArray = self.restore_cluster_sub(node_id, token, topology_name, domain, node_name,**kwargs)

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            return retArray

        except Exception,e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()) )

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            #raise
            return [NG, traceback.format_exc()]

            raise
    """

    #####################
    #Restore Module
    #####################
    def restore_cluster_sub(self, node_id, token, topology_name, domain, node_name, **kwargs):

        #####################
        #R predefine
        #####################
        br_mode = "r"
        CLSTER_NAME = "TS_" + domain + "_" + topology_name

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Restore Start ')

        if not 'restore_name' in kwargs:
            return ['NG', 'restore folder name is required']

        #get restore folder name
        restore_folder_name = self.get_restore_foldername(**kwargs)

        BACKUP_FOLDER_RNAME = CLSTER_NAME + "_ID%s" % (node_id) + "/" + restore_folder_name
        BACKUP_IMGNAME_PRE = restore_folder_name

        self.br_log(node_id, CLSTER_NAME, br_mode, '###  Input Parameter  ####')
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_id              : %s' % node_id)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'token                : %s' % token)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'topology_name        : %s' % topology_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'domain               : %s' % domain)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_name            : %s' % node_name)
        self.br_log(node_id, CLSTER_NAME, br_mode,
                    'kwargs[restore_name]  : %s' % (kwargs['restore_name']))
        self.br_log(node_id, CLSTER_NAME, br_mode, 'restore_folder: %s' % BACKUP_FOLDER_RNAME)

        #####################
        #R define
        #####################
        SAVE_DIR_NAME = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/server"
        SAVE_DIR_NAME_SWITCH = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/switch"

        NODE_INFO_FILE_NAME = "node_info"
        NODE_LIST_FILE = BASE_DIR_NAME + "/" + BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, token)

        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return ['NG', '###get token ng']

        ######################
        #R Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        #make storage server save val
        server_cnt = 1
        server_info_num = 5
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ################
        #R Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
        ret = self.get_user_name(self.opencenter_server_name)
        if 0 == ret[0]:
            EXEC_USER = ret[1]
        else:
            self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
            msg = 'Set Exec_User error'
            return ['NG', msg]

        ########################
        #R Set Storage Server info
        ########################
        ret, msg = self.set_server_info_env(
            node_id, CLSTER_NAME, br_mode,
            [self.clonezilla_server_name], server_info, 1, EXEC_USER)

        if OK != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
            return ['NG', msg]

        #######################
        #R Get backup ServerInfo
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Get backup ServerInfo ')

        d = datetime.datetime.today()
        tm = d.strftime("%H%M%S")
        file_path = '/tmp/' + NODE_INFO_FILE_NAME + tm

        cmd = "scp openstack@%s:%s %s  " %\
            (server_info[CLONEZILLA_SV][IP_INDEX], NODE_LIST_FILE, file_path)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            NODE_LIST_FILE
            msg = 'R node_info trans err'
            return ['NG', msg]

        f = open(file_path, 'r')
        backup_node_info = json.load(f)
        f.close()

        cmd = "rm %s " % (file_path)
        commands.getoutput(cmd)

        self.br_log(node_id, CLSTER_NAME, br_mode,
                    '#### R Get backup ServerInfo :%s' % (backup_node_info))

        ########################
        #R Get server name for backupfile
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for storage server')
        ret_restore_info = self.get_node_info_from_serverFile(
            node_id, CLSTER_NAME, br_mode, backup_node_info)

        print ret_restore_info
        server_node_name = ret_restore_info[SV_NAME]
        server_num = len(server_node_name)
        server_cnt = server_num + 1

        tmp_server_info = server_info[CLONEZILLA_SV]

        ###################
        #resize server info
        ###################
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]
        server_info[CLONEZILLA_SV] = tmp_server_info

        switch_node_name = ret_restore_info[SW_NAME]
        switch_num = len(switch_node_name)
        switch_node_name_char = ','.join(switch_node_name)

        if (0 == server_num) and (0 == switch_num):
            self.br_log(
                node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
            return ['OK', 'Restore ok']

        ########################
        #R Check Cluster node
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Check Cluster node')

        backupedCluster_node = server_node_name + switch_node_name
        nowClaster_node = node_name

        ret = self.check_novacluster_node(
            CLSTER_NAME, node_id, br_mode, backupedCluster_node, nowClaster_node)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#R Check Cluster node  check ng')
            msg = 'R Check Cluster node  check ng'
            return ['NG', msg]

        ########################
        #R Set NovaClaster Server info env
        ########################
        ret, msg = self.set_server_info_env(
            node_id, CLSTER_NAME, br_mode,
            server_node_name, server_info[1:], server_num, EXEC_USER)
        if OK != ret:
            self.br_log(
                node_id, CLSTER_NAME, br_mode,
                '#### Set Server info  server_node_name=%s' % (server_node_name[i-1]))
            return ['NG', msg]

        #######################
        #R Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        cmd = 'ssh openstack@%s ls -d  %s 2> /dev/null' %\
            (server_info[CLONEZILLA_SV][IP_INDEX],  SAVE_DIR_NAME)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '#### Check Directory Err %s is no directory' % (SAVE_DIR_NAME))
            msg = 'Check Directory '
            return ['NG', msg]

        #DDD debug DDD
        #switch_num=0

        #DDD debug
        if DEBUG == "ON":
            switch_num = 0
        #################
        #R switch restore
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Switch restore Start')

            psbk = psbk_manager.psbk_manager(
                EXEC_USER, self.clonezilla_server_name, SAVE_DIR_NAME_SWITCH, self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH Call psbk.set_PS_list')
            self.br_log(node_id, CLSTER_NAME, br_mode,
                        '####SWITCH_NODE_LIST :List(char) %s ' % (switch_node_name_char))

            ret = psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH psbk.set_PS_list Err')
                msg = '#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)'
                return ['NG', msg]

            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
            ret = psbk.exec_restore()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
                msg = '####SWITCH  psbk.exec_restore() '
                return ['NG', msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch restore End')

        #DDD
        if DEBUG == "ON":
            server_num = 0

        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            return ['OK', 'Restore ok']

        #######################
        #R Del Restore Status File
        #######################
        cmd = 'ssh openstack@%s rm -rf %s/status_r*  2> /dev/null' %\
            (server_info[CLONEZILLA_SV][IP_INDEX], SAVE_DIR_NAME)
        ret = self.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Del Restore Status File Err ')
            msg = 'Del Restore Status File Err '
            return ['NG', msg]

        ################
        #R Run Restore
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Restore 2')

        clonezilla_info = {}
        clonezilla_info['ip_address'] = server_info[CLONEZILLA_SV][IP_INDEX]
        clonezilla_info['username'] = server_info[CLONEZILLA_SV][USER_INDEX]
        clonezilla_info['password'] = server_info[CLONEZILLA_SV][PW_INDEX]

        data_info = []
        for data in server_info[START_INDEX:]:
            tmp_data = {}
            tmp_data['hostname'] = data[NAME_INDEX]
            tmp_data['ip_address'] = data[IP_INDEX]
            tmp_data['username'] = data[USER_INDEX]
            tmp_data['password'] = data[PW_INDEX]
            tmp_data['ip_address_c'] = data[C_IP_INDEX]
            data_info.append(tmp_data)

        ret = fuel_utls.clonezilla_exec(
            self.token, fuel_utls.MODE_RESTORE,
            clonezilla_info, data_info,
            server_cnt - 1, BACKUP_IMGNAME_PRE)

        if ret != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Clonezilla server Restore Error')
            msg = 'Clonezilla server Restore Error'
            return ['NG', msg]

        for server in data_info:
            ret = self.wait_os_startup(
                server['hostname'], server['username'], server['password'])
            if ret == -1:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### wait_os_startup Error')

        #################
        # restore server type
        #################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB server type')
        ret = self.setdb_server_type(CLSTER_NAME, node_id, br_mode, kwargs['restore_name'])
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB server type ng')
            msg = '#### set DB server type ng '
            return ['NG', msg]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')

        return ['OK', 'Restore ok']

# Fuel --------
    """
    def node_reboot(self, node_info):
        try:
            s2 = pxssh.pxssh()
            s2.login(node_info[IP_INDEX], node_info[USER_INDEX], node_info[PW_INDEX])
            s2.sendline('sudo reboot')
            s2.expect('.*password for .*')
            s2.sendline(node_info[PW_INDEX])
            s2.prompt()
            s2.logout()

            return 0

        except pxssh.ExceptionPxssh, e:
            print "pxssh failed on login."
            print str(e)

        except pexpect.EOF:
            # use bmc port reboot
            print "pxssh failed on login. use bmc reboot"

            # Get Network info
            nicinfo = fuel_utls.node_nic_info(self.Token, node_info[NAME_INDEX])
            server_bip = nicinfo.get_ip_address(nicinfo.B_PLANE)
            if (server_bip == -1):
                self.svbkm.b_log(
                    "", self.topology_name, '#### reset_fuelnode_reboot get_nic Error')
                return -1
            else:
                print server_bip
                #server_bip = '172.16.1.180'

            ipmi = ipmi_client.IpmiClient()
            #ipmi.run_reboot(server_bip)
            ret = ipmi.get_all_status(server_bip)
            print ret
    """
    """
    def clonezilla_exec(self, server_info, server_cnt, mode, img_pre):

        if MODE_RESTORE == mode:
            cmd_format = 'sudo drbl-ocs -b -g auto -e1 auto -e2 -r -x -j2 -p ' \
                'reboot -h "%s" -l ja_JP.UTF-8 startdisk restore %s sda'
        else:
            cmd_format = 'sudo drbl-ocs -b -q2 -j2 -fsck-src-part-y -sc -p ' \
                'reboot -z1p -i 1000000 -h "%s" -l ja_JP.UTF-8 startdisk save %s sda'

        clonezilla_serverip = server_info[CLONEZILLA_SV][IP_INDEX]
        clonezilla_username = server_info[CLONEZILLA_SV][USER_INDEX]
        clonezilla_password = server_info[CLONEZILLA_SV][PW_INDEX]

        try:
            s = pxssh.pxssh()
            s.login(clonezilla_serverip, clonezilla_username, clonezilla_password)

            # sudo dummy command
            cmd = "sudo pwd"
            s.sendline(cmd)
            s.expect('.*password for .*')
            s.sendline(clonezilla_password)
            s.prompt()

            chk_string = []
            for i in range(START_INDEX, server_cnt):
                server_cip = server_info[i][C_IP_INDEX]
                seve_img = img_pre + "_" + server_info[i][NAME_INDEX]
                cmd = cmd_format % (server_cip, seve_img)
                print cmd
                #--- Fuel Server image restore command to Clonsezilla
                s.sendline(cmd)
                s.prompt()

                chk_string.append('client ' + server_cip)

                #--- reboot Fuel node
                fuel_utls.node_reboot(
                    self.token,
                    server_info[i][NAME_INDEX],
                    server_info[i][USER_INDEX],
                    server_info[i][PW_INDEX],
                    server_info[i][IP_INDEX])

            chk_string.append("dummy 0.0.0.0")
            print chk_string

            #--- wait for finish restore
            s.expect('You are in clonezilla box mode!')
            s.sendline('tail -f /var/log/clonezilla/clonezilla-jobs.log')

            chk_server_cnt = server_cnt - START_INDEX
            #print 'chk_server_cnt:' +str(chk_server_cnt)
            while chk_server_cnt > 0:
                #print chk_string
                # todo timeout set time & timeout process
                s.expect(chk_string, timeout=20*60)
                #print "s.after:" + s.after
                #print "s.before:" + s.before
                #print "s.buffer:" + s.buffer
                chk_server_cnt -= 1

            s.sendcontrol('c')
            s.prompt()
            s.sendline('cat /var/log/clonezilla/clonezilla-jobs.log')
            s.prompt()
            logs = s.before
            print logs

            s.logout()

        except pxssh.ExceptionPxssh, e:
            print "pxssh failed on login."
            print str(e)

        if "error" in logs:
            # todo err message
            #self.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

            return -1

        return 0
    """

    def set_server_info_env(self, node_id, clster_name, br_mode,
                            server_node_name, server_info, server_cnt, exec_user):

        ########################
        # Set Server info
        ########################
        self.br_log(node_id, clster_name, br_mode, '#### Set Server info')

        for i in range(server_cnt):
            self.br_log(node_id, clster_name, br_mode,
                        'Set Server info server_node_name[%s]=%s' %
                        (i, server_node_name[i]))

            retdata = self.set_server_info(
                node_id, server_info[i], server_node_name[i], clster_name, br_mode)
            if 0 != retdata:
                self.br_log(node_id, clster_name, br_mode,
                            '#### Set Server info  server_node_name=%s' % (server_node_name[i]))
                msg = 'Set Server info set server_node_name=%s' % (server_node_name[i])
                return NG, msg

        #####################################
        # Set Server Make Exec Env
        #####################################
        self.br_log(node_id, clster_name, br_mode, '#### Set Server Make Exec Env')

        for i in range(server_cnt):
            self.br_log(
                node_id, clster_name, br_mode,
                'backup exec start all=%d index=%d' % (server_cnt, i))
            ret = self.make_exec(
                exec_user, server_info[i][IP_INDEX],
                server_info[i][USER_INDEX], server_info[i][PW_INDEX],
                FILEDIR, br_mode, node_id, clster_name)
            if ret != 0:
                self.br_log(
                    node_id, clster_name, br_mode,
                    '#### backup exec start err all=%d index=%d' % (server_cnt, i))

                msg = 'make_exec err'
                return NG, msg

        return OK, 'Success'
