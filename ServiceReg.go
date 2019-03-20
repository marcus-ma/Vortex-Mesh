package CommentService

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"log"
	"context"
	"errors"
	"fmt"
	"encoding/json"
	"strconv"
)

var (
	//单例对象
	G_serReg *Service
)

//要由外部传入的配置信息
type RegConf struct {
	//服务名称
	ServiceName string
	//服务IP
	ServiceIP string

	//etcd集群
	EtcdEndpoints []string
	//连接超时
	EtcdDialTimeout int
	//键的前缀
	EtcdServiceKeyPrefix string
}

type ServiceInfo struct {
	IP string
}

type Service struct {
	Name string
	Info ServiceInfo
	stop chan error
	leaseId clientv3.LeaseID
	client *clientv3.Client
}

func NewService(name string,info ServiceInfo,endpoints []string,dialTimeout int) (*Service,error) {
	var (
		err error
		conf clientv3.Config
		cli *clientv3.Client
	)

	conf = clientv3.Config{
		Endpoints:endpoints,
		DialTimeout:time.Duration(dialTimeout)*time.Millisecond,
	}

	if cli,err = clientv3.New(conf);err!=nil{
		log.Fatal(err)
		return nil,err
	}

	return &Service{
		Name:    name,
		Info:    info,
		stop:    make(chan error),
		client:  cli,
	},err

}

func (s *Service) Start(serviceKeyPrefix string) error {
	var (
		err error
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		keepResp *clientv3.LeaseKeepAliveResponse
		ok bool
	)

	if keepRespChan,err = s.keepAlive(serviceKeyPrefix);err!=nil{
		log.Fatalln(err)
		return err
	}

	for  {
		select {
		//服务逻辑上自己停止(设置定时器after之类，到期执行stop方法)
		case err = <- s.stop:
			s.revoke()
			return err
			//整个服务突然关闭
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
			//收到注册中心的回应
		case keepResp,ok = <-keepRespChan:
			if !ok {
				log.Println("keep alive channel close")
				s.revoke()
				return nil
			}else {
				log.Printf("Recv reply from service:%s,ttl:%d",s.Name,keepResp.TTL)
			}
		}
	}
}

func (s *Service) Stop()  {
	s.stop <-nil
}

func (s *Service) keepAlive(serviceKeyPrefix string) (<-chan *clientv3.LeaseKeepAliveResponse,error) {
	var (
		err error
		info *ServiceInfo
		key string
		value []byte
		leaseGrantResp *clientv3.LeaseGrantResponse
	)

	//申请5秒生存期的租约,得到租约id
	if leaseGrantResp,err = s.client.Grant(context.TODO(),5);err!=nil{
		log.Fatal(err)
		return nil,err
	}

	//服务的k
	//key = serviceKeyPrefix + s.Name + "/"+  fmt.Sprintf("%d", leaseGrantResp.ID)
	key = serviceKeyPrefix + s.Name

	info = &s.Info
	//服务具体信息
	value,_ = json.Marshal(info)


	//将服务信息注册到etcd上
	if _,err = s.client.Put(context.TODO(),key,string(value),clientv3.WithLease(leaseGrantResp.ID));err!=nil{
		log.Fatal(err)
		return nil,err
	}


	//将租约id绑定到服务结构体中,可用于在服务关闭后将让自动续租停止
	s.leaseId = leaseGrantResp.ID
	//开启心跳包发送，定时每秒发送续租请求
	return s.client.KeepAlive(context.TODO(),leaseGrantResp.ID)
}

func (s *Service) revoke () error {
	var err error
	//停止续租
	if _,err = s.client.Revoke(context.TODO(),s.leaseId);err!=nil{
		log.Fatal(err)
	}
	log.Printf("servide:%s stop\n",s.Name)
	return err
}



func InitSerReg() (err error) {
	var (
		regConf *RegConf
		serviceInfo ServiceInfo
		service *Service
	)

	//配置信息
	regConf = &RegConf{
		ServiceName : G_config.ServiceName,
		ServiceIP :G_config.ServiceIP+ ":" + strconv.Itoa(G_config.ApiPort),
		EtcdEndpoints : G_config.EtcdEndpoints,
		EtcdDialTimeout : G_config.EtcdDialTimeout,
		EtcdServiceKeyPrefix : G_config.EtcdServiceKeyPrefix,
	}

	//服务的信息
	serviceInfo = ServiceInfo{IP:regConf.ServiceIP}

	if service,err = NewService(
		regConf.ServiceName,
		serviceInfo,
		regConf.EtcdEndpoints,
		regConf.EtcdDialTimeout); err!=nil{
			log.Fatal(err)
			return
	}

	fmt.Printf("name: %s,ip:%s\n",service.Name,service.Info.IP)
	go service.Start(regConf.EtcdServiceKeyPrefix)

	G_serReg = service

	return

}