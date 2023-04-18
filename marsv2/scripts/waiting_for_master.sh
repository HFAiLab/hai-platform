# This script is used to get ip address of master node, which defaults to rank 0
# OS environment MASTER_IP will be written into /etc/bash.bashrc
MASTER_DNS="${MARSV2_USER//_/-}-${MARSV2_TASK_ID}-0"
resolve_count=1
echo "Start resloving DNS of master node '${MASTER_DNS}'" | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
while true;do
    # Avoid command failure when shell option sets with -e
    MASTER_IP=`timeout 1 dig -t A -4 +search +short ${MASTER_DNS} || echo $?`;
    if [[ $MASTER_IP =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]];then
        echo "Get Master IP: '${MASTER_IP}'" | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
        export MASTER_ADDR=${MASTER_IP};
        export MASTER_IP=${MASTER_IP};
        echo "export MASTER_ADDR=${MASTER_IP}" >> /etc/bash.bashrc
        echo "export MASTER_IP=${MASTER_IP}" >> /etc/bash.bashrc
        break;
    fi;
    echo "Resolve count "${resolve_count}", Get invalid IP: '$MASTER_IP'" | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH};
    let resolve_count+=1
    sleep 1;
done

let MASTER_PORT+=1
