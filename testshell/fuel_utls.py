# -*- coding: utf-8 -*-
import ool_rm_if
import pexpect
import pxssh
import ipmi_client
import json
import urllib2
import time
from retry import retry

MODE_BACKUP = "backup"
MODE_RESTORE = "restore"


class node_nic_info:

    # Nic Traffic Tyep
    M_PLANE = '002'
    B_PLANE = '005'
    M2_PLANE = '006'
    UNUSED = '999'

    def __init__(self, tokenid, hostname):
        ori = ool_rm_if.ool_rm_if()
        ori.set_auth(tokenid)
        self.data = ori.get_nic(hostname)
        if -1 == self.data[0]:
            print "error"

    def get_ip_address(self, planename):

        ipaddress = -1
        if -1 != self.data[0]:
            for info in self.data[1]:
                if info['traffic_type'] == planename:
                    ipaddress = info['ip_address']

        return ipaddress

    def get_gw_address(self, planename):

        gateway = -1
        if -1 != self.data[0]:
            for info in self.data[1]:
                if info['traffic_type'] == planename:
                    if 'gateway' in info:
                        gateway = info['gateway']

        return gateway


def node_reboot(tokenid, hostname, username, password, ip_address=''):

    if ip_address == '':
        nicinfo = node_nic_info(tokenid, hostname)
        ip_address = nicinfo.get_ip_address(nicinfo.M_PLANE)
        if (ip_address == -1):
            print '#### node_reboot get_nic Error'
            return -1

    try:
        s2 = pxssh.pxssh()
        s2.login(ip_address, username, password)
        s2.sendline('sudo reboot')
        s2.expect('.*password for .*', timeout=2*600)
        s2.sendline(password)
        s2.prompt()
        s2.logout()

        return 0

    except pxssh.ExceptionPxssh, e:
        print "pxssh failed on login."
        print str(e)
        print "use bmc reboot."
        ret = bmc_reboot(tokenid, hostname)
        return ret

    except pexpect.EOF:
        # use bmc port reboot
        print "pxssh failed on login. use bmc reboot"

        ret = bmc_reboot(tokenid, hostname)
        return ret


def bmc_reboot(tokenid, hostname):

        nicinfo = node_nic_info(tokenid, hostname)
        bmc_address = nicinfo.get_ip_address(nicinfo.B_PLANE)
        if (bmc_address == -1):
            print "node_reboot get_nic Error(bmc)"
            return -1

        ipmi = ipmi_client.IpmiClient()
        ret = ipmi.run_reboot(bmc_address)
        if 'error' == ret:
            ret = ipmi.run_reset(bmc_address)
            if 'error' == ret:
                print "node_reboot bmc reboot error"
                return -1


def clonezilla_exec(tokenid, mode, clonezilla_info, server_info, server_cnt, img_pre=''):

    if MODE_RESTORE == mode:
        cmd_format = 'sudo drbl-ocs -b -g auto -e1 auto -e2 -r -x -j2 -p ' \
            'reboot -h "%s" -l ja_JP.UTF-8 startdisk restore %s sda'
    else:
        cmd_format = 'sudo drbl-ocs -b -q2 -j2 -fsck-src-part-y -sc -p ' \
            'reboot -z1p -i 1000000 -h "%s" -l ja_JP.UTF-8 startdisk save %s sda'

    clonezilla_serverip = clonezilla_info['ip_address']
    clonezilla_username = clonezilla_info['username']
    clonezilla_password = clonezilla_info['password']

    try:
        s = pxssh.pxssh()
        s.login(clonezilla_serverip, clonezilla_username, clonezilla_password)

        # sudo dummy command
        cmd = "sudo pwd"
        s.sendline(cmd)
        s.expect('.*password for .*')
        s.sendline(clonezilla_password)
        s.prompt()

        for i in range(server_cnt):
            server_cip = server_info[i]['ip_address_c']
            if img_pre != '':
                save_img = img_pre + "_" + server_info[i]['hostname']
            else:
                save_img = server_info[i]['img_name']

            cmd = cmd_format % (server_cip, save_img)
            print cmd
            # --- Fuel Server image restore command to Clonsezilla
            s.sendline(cmd)
            s.prompt()
            # clonezilla bug wait next command
            time.sleep(30)

            # --- reboot Fuel node
            ret = node_reboot(
                tokenid,
                server_info[i]['hostname'],
                server_info[i]['username'],
                server_info[i]['password'],
                server_info[i]['ip_address'])

            if ret == -1:
                print 'xxx reboot error'
                return -1

        s.logout()

        time.sleep(8*60)
        for server in server_info:
            wait_start_node(server['ip_address'], server['username'], server['password'])

        return 0

    except pxssh.ExceptionPxssh, e:
        print "pxssh failed on login."
        print str(e)


@retry(pexpect.EOF, tries=100, delay=20)
def wait_start_node(server_mip, username, password):

    try:
        s = pxssh.pxssh()
        s.login(server_mip, username, password, login_timeout=10*60)
        s.logout()

    except pxssh.ExceptionPxssh, e:
        print "pxssh failed on login."
        print str(e)

    return 0


def http_request(url, params, cmd):

    # リクエストヘッダ
    headers = {"Content-Type": "application/json"}

    post_param = json.dumps(params)

    # リクエストを生成
    request = urllib2.Request(url, post_param, headers)

    request.get_method = lambda: cmd

    # リクエストを送信して結果受け取り
    r = urllib2.urlopen(request)

    print r.code
    print r.msg
    return r.code, r.msg
