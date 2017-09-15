#!/bin/bash

HOST_NAME=hdp.learningjournal.local
SHORT_NAME=hdp
read dummy IP_ADDRESS <<< $(hostname -I)
echo $IP_ADDRESS $HOST_NAME $SHORT_NAME >> /etc/hosts
sed -i "s/HOSTNAME=localhost.localdomain/HOSTNAME=$HOST_NAME/g" /etc/sysconfig/network
hostname $HOST_NAME
service network restart

ssh-keygen
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys

ulimit -n 10000
service ntpd start
chkconfig ntpd on
service iptables stop
chkconfig iptables off
service ip6tables stop
chkconfig ip6tables off
setenforce 0
sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
sed -i 's/enabled=1/enabled=0/g' /etc/yum/pluginconf.d/refresh-packagekit.conf
service httpd start
chkconfig httpd on
service sshd start
chkconfig sshd on
cat thp.txt >> /etc/rc.local

tar -zxvf ambari-2.4.2.0-centos6.tar.gz -C /var/www/html/
tar -zxvf HDP-UTILS-1.1.0.21-centos6.tar.gz -C /var/www/html/
tar -zxvf HDP-2.5.3.0-centos6-rpm.tar.gz -C /var/www/html/
cp *.repo /etc/yum.repos.d/
reboot
